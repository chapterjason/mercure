package hub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/go-redis/redis/v8"
	"go.uber.org/atomic"
)

var ctx = context.Background()

const defaultRedisKeyName = "mercure-hub-updates"

func redisNilToNil(err error) error {
	if err == redis.Nil {
		return nil
	}
	return err
}

// RedisTransport implements the TransportInterface using a Redis server.
type RedisTransport struct {
	sync.RWMutex
	client      *redis.Client
	keyName     string
	size        uint64
	subscribers map[*Subscriber]struct{}
	closed      chan struct{}
	closedOnce  sync.Once
	listen      bool
	lastSeq     atomic.String
	lastEventID string
}

// NewRedisTransport create a new RedisTransport.
func NewRedisTransport(u *url.URL) (*RedisTransport, error) {
	var err error
	q := u.Query()
	keyName := defaultRedisKeyName
	if q.Get("key_name") != "" {
		keyName = q.Get("key_name")
		q.Del("key_name")
	}

	masterName := ""
	if q.Get("master_name") != "" {
		masterName = q.Get("master_name")
		q.Del("master_name")
	}

	size := uint64(0)
	sizeParameter := q.Get("size")
	if sizeParameter != "" {
		size, err = strconv.ParseUint(sizeParameter, 10, 64)
		if err != nil {
			return nil, fmt.Errorf(`%q: invalid "size" parameter %q: %s: %w`, u, sizeParameter, err, ErrInvalidTransportDSN)
		}
		q.Del("size")
	}

	u.RawQuery = q.Encode()

	redisOptions, err := redis.ParseURL(u.String())
	if err != nil {
		return nil, fmt.Errorf(`%q: invalid "redis" dsn: %w`, u, ErrInvalidTransportDSN)
	}

	var client *redis.Client
	if masterName != "" {
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:    masterName,
			DB:            redisOptions.DB,
			Password:      redisOptions.Password,
			SentinelAddrs: []string{redisOptions.Addr},
		})
	} else {
		client = redis.NewClient(redisOptions)
	}

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf(`%q: redis connection error "%s": %w`, u, err, ErrInvalidTransportDSN)
	}

	return &RedisTransport{
		client:      client,
		keyName:     keyName,
		size:        size,
		listen:      false,
		subscribers: make(map[*Subscriber]struct{}),
		closed:      make(chan struct{}),
		lastEventID: getLastEventID(client, keyName),
	}, nil
}

////////////////////////////////////////////////////////////////////////
////// Global utils
////////////////////////////////////////////////////////////////////////
func getLastEventID(client *redis.Client, keyName string) string {
	lastEventID := EarliestLastEventID
	messages, err := client.XRevRangeN(ctx, keyName, "+", "-", 1).Result()

	if err != nil {
		return lastEventID
	}

	for _, entry := range messages {
		lastEventID = entry.ID
	}

	return lastEventID
}

////////////////////////////////////////////////////////////////////////
////// Main
////////////////////////////////////////////////////////////////////////

// Dispatch dispatches an update to all subscribers and persists it in BoltDB.
func (t *RedisTransport) Dispatch(update *Update) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.ensureListen()

	AssignUUID(update)
	updateJSON, err := json.Marshal(*update)
	if err != nil {
		return err
	}

	t.Lock()
	// Remove subscriber if disconnected
	for subscriber := range t.subscribers {
		select {
		case <-subscriber.disconnected:
			close(subscriber.live.in)
			delete(t.subscribers, subscriber)
			continue
		default:
		}
	}
	t.Unlock()

	if err := t.persist(update.ID, updateJSON); err != nil {
		return err
	}

	t.ensureListen()

	return nil
}

// AddSubscriber adds a new subscriber to the transport.
func (t *RedisTransport) AddSubscriber(s *Subscriber) error {
	select {
	case <-t.closed:
		return ErrClosedTransport
	default:
	}

	t.ensureListen()

	t.Lock()
	t.subscribers[s] = struct{}{}
	toSeq := t.lastSeq.Load()
	t.Unlock()

	if s.RequestLastEventID != "" {
		t.dispatchHistory(s, toSeq)
	}

	t.ensureListen()

	return nil
}

// GetSubscribers get the list of active subscribers.
func (t *RedisTransport) GetSubscribers() (lastEventID string, subscribers []*Subscriber) {
	t.RLock()
	defer t.RUnlock()
	subscribers = make([]*Subscriber, len(t.subscribers))

	i := 0
	for subscriber := range t.subscribers {
		subscribers[i] = subscriber
		i++
	}

	return t.lastEventID, subscribers
}

// Close closes the Transport.
func (t *RedisTransport) Close() (err error) {
	t.closedOnce.Do(func() {
		close(t.closed)

		t.Lock()
		for subscriber := range t.subscribers {
			subscriber.Disconnect()
			delete(t.subscribers, subscriber)
		}
		t.Unlock()

		err = t.client.Close()
	})

	return err
}

////////////////////////////////////////////////////////////////////////
////// extra
////////////////////////////////////////////////////////////////////////

func (t *RedisTransport) Flush() {
	result, err := t.client.FlushAll(ctx).Result()

	if redisNilToNil(err) != nil {
		log.Error(fmt.Errorf("[Redis] Flush Error: %w\n", err))
		return
	}

	log.Info(fmt.Printf("[Redis] Flush: %s\n", result))
}

func (t *RedisTransport) ensureListen() {
	if !t.listen {
		lastSeq, err := t.fetchLastSequence()
		if err != nil {
			log.Error(fmt.Errorf("[Redis] EnsureListen fetchLastSequence Error: %w\n", err))
		} else {
			if lastSeq == "" {
				lastSeq = "$"
			}

			t.listen = true

			t.lastSeq.Store(lastSeq)
			go t.listenMessages()
		}
	}
}

func (t *RedisTransport) cacheKeyID(ID string) string {
	return fmt.Sprintf("%s/%s", t.keyName, ID)
}

// persist stores update in the database.
func (t *RedisTransport) persist(updateID string, updateJSON []byte) error {
	var script string
	if t.size > 0 {
		// Script Explanation
		// Convert the <Arg:History Size> into a number
		// Add to <Key:Stream Name> using Auto-Generated Entry ID, Limiting the length to <Arg:History Size> add an entry with the data key set to <Arg:Update JSON> and return <res:Entry ID>
		// Add to the end of the <Key:cacheKeyId(updateID)> List the <res:Entry ID>
		// Add to the end of the <Key:cacheKeyId("") List the <Key:cacheKeyId(updateID)>
		// While the length of the <Key:cacheKeyId("")> List is over <Arg:History Size>
		//  - Get the first key in the list
		//  - Remove it from the list
		//  - If the length of that list is 0
		//     - Delete that key

		script = `
			local limit = tonumber(ARGV[1])
			local entryId = redis.call("XADD", KEYS[1], "*", "MAXLEN", ARGV[1], "data", ARGV[2])
			redis.call("RPUSH", KEYS[2], entryId)
			redis.call("RPUSH", KEYS[3], KEYS[2])
			while (redis.call("LLEN", KEYS[3]) > limit) do
				local key = redis.call("LPOP", KEYS[3])
				redis.call("LPOP", key)
				if redis.call("LLEN", key) == 0 then
					redis.call("DEL", key)
				end
			end`
	} else {
		script = `
			local streamID = redis.call("XADD", KEYS[1], "*", "data", ARGV[2])
			redis.call("RPUSH", KEYS[2], streamID)`
	}

	log.Info(fmt.Printf("[Redis] Save Update ID: %s, Cache Key: %s, Stream: %s\n", updateID, t.cacheKeyID(updateID), t.keyName))

	err := t.client.Eval(ctx, script, []string{
		t.keyName,
		t.cacheKeyID(updateID),
		t.cacheKeyID(""),
	}, t.size, updateJSON).Err()

	if redisNilToNil(err) != nil {
		return err
	}

	return nil
}

func (t *RedisTransport) fetchLastSequence() (string, error) {
	messages, err := t.client.XRevRangeN(ctx, t.keyName, "+", "-", 1).Result()
	if err != nil {
		return "", redisNilToNil(err)
	}

	for _, entry := range messages {
		return entry.ID, nil
	}

	return "", nil
}

func (t *RedisTransport) dispatchHistory(s *Subscriber, toSeq string) {
	var fromSeq = s.RequestLastEventID
	if toSeq == "" {
		toSeq = "+"
	}

	if fromSeq != EarliestLastEventID {
		var err error
		fromSeq, err = t.client.LIndex(ctx, t.cacheKeyID(fromSeq), 0).Result()
		if redisNilToNil(err) != nil {
			log.Error(fmt.Errorf("[Redis] Dispatch History List Index Error: %w\n", err))
			s.HistoryDispatched(EarliestLastEventID)
			return // No data
		}
		log.Info(fmt.Printf("[Redis] Dispatch History List Index result: %s\n", fromSeq))

	} else {
		fromSeq = "-"
	}

	log.Info(fmt.Printf("[Redis] Searching Stream for records FROM: %s, TO: %s\n", fromSeq, toSeq))
	messages, err := t.client.XRange(ctx, t.keyName, fromSeq, toSeq).Result()
	if err != nil {
		log.Error(fmt.Errorf("[Redis] XRange error: %w", err))
		s.HistoryDispatched(EarliestLastEventID)
		return // No data
	}

	responseLastEventID := fromSeq
	var first = true
	for _, entry := range messages {
		// skip first message if not -
		if fromSeq == "-" {
			first = false
		}

		if first {
			first = false
			continue
		}

		message, ok := entry.Values["data"]
		if !ok {
			s.HistoryDispatched(responseLastEventID)
			log.Error(fmt.Errorf("[Redis] Read History Entry Error: %w\n", err))
			return
		}

		var update *Update
		if err := json.Unmarshal([]byte(fmt.Sprintf("%v", message)), &update); err != nil {
			s.HistoryDispatched(responseLastEventID)
			log.Error(fmt.Errorf(`[Redis] stream return an invalid entry: %v\n`, err))
			return
		}

		if !s.Dispatch(update, true) {
			s.HistoryDispatched(responseLastEventID)
			log.Error(fmt.Errorf("[Redis] Dispatch error: %w\n", err))
			return
		}
		responseLastEventID = update.ID
	}

	s.HistoryDispatched(responseLastEventID)
	return
}

////////////////////////////////////////////////////////////////////////
////// stream listen
////////////////////////////////////////////////////////////////////////

func (t *RedisTransport) listenMessages() {
	for {
		select {
		case <-t.closed:
			t.listen = false
			return
		default:
		}

		if err := t.readMessages(); err != nil {
			log.Error(fmt.Errorf(`[Redis] listenMessages: %v\n`, err))
			time.Sleep(20 * time.Millisecond) // avoid infinite loop consuming CPU
		}
	}
}

func (t *RedisTransport) readMessages() error {
	var fromSeq = t.lastSeq.Load()
	var block = time.Second / time.Millisecond

	streams, err := t.client.XRead(ctx, &redis.XReadArgs{
		Streams: []string{t.keyName, fromSeq},
		Count:   1,
		Block:   block,
	}).Result()

	if err != nil {
		return redisNilToNil(err)
	}

	for _, stream := range streams {
		for _, entry := range stream.Messages {
			if err := t.processMessage(entry); err != nil {
				log.Error(fmt.Errorf(`[Redis] processMessage: %v\n`, err))
				return err
			}
			t.lastSeq.Store(entry.ID)
		}
	}

	return nil
}

func (t *RedisTransport) processMessage(entry redis.XMessage) error {
	message, ok := entry.Values["data"]
	if !ok {
		return fmt.Errorf(`stream returns an invalid entry: %v`, entry.Values)
	}

	log.Info(fmt.Printf("[Redis] processMessage: %s\n", message))

	var update *Update
	if err := json.Unmarshal([]byte(fmt.Sprintf("%v", message)), &update); err != nil {
		return err
	}

	t.Lock()
	defer t.Unlock()

	for subscriber := range t.subscribers {
		if subscriber.CanDispatch(update) {
			if !subscriber.Dispatch(update, false) {
				delete(t.subscribers, subscriber)
			}
		}
	}

	t.lastEventID = update.ID

	return nil
}
