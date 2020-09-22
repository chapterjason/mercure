package hub

import (
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"strconv"
	"sync"
	"testing"
)

func TestRedisTransportHistory(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	defer transport.Close()
	defer transport.Flush()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewSubscriber("8", NewTopicSelectorStore())
	s.Topics = topics
	go s.start()

	require.Nil(t, transport.AddSubscriber(s))

	var count int
	for {
		u := <-s.Receive()
		// the reading loop must read the #9 and #10 messages
		assert.Equal(t, strconv.Itoa(9+count), u.ID)
		count++
		if count == 2 {
			return
		}
	}
}

func TestRedisTransportRetrieveAllHistory(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	defer transport.Close()
	defer transport.Flush()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewSubscriber(EarliestLastEventID, NewTopicSelectorStore())
	s.Topics = topics
	go s.start()
	require.Nil(t, transport.AddSubscriber(s))

	var count int
	for {
		u := <-s.Receive()
		// the reading loop must read all messages
		count++
		assert.Equal(t, strconv.Itoa(count), u.ID)
		if count == 10 {
			return
		}
	}
}

func TestRedisTransportHistoryAndLive(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	defer transport.Close()
	defer transport.Flush()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 10; i++ {
		transport.Dispatch(&Update{
			Topics: topics,
			Event:  Event{ID: strconv.Itoa(i)},
		})
	}

	s := NewSubscriber("8", NewTopicSelectorStore())
	s.Topics = topics
	go s.start()
	require.Nil(t, transport.AddSubscriber(s))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var count int
		for {
			u := <-s.Receive()

			// the reading loop must read the #9, #10 and #11 messages
			assert.Equal(t, strconv.Itoa(9+count), u.ID)
			count++
			if count == 3 {
				return
			}
		}
	}()

	transport.Dispatch(&Update{
		Event:  Event{ID: "11"},
		Topics: topics,
	})

	wg.Wait()
}

func TestRedisTransportPurgeHistory(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379?size=5")
	transport, _ := NewRedisTransport(u)
	defer transport.Close()
	defer transport.Flush()

	for i := 0; i < 12; i++ {
		transport.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: []string{"https://example.com/foo"},
		})
	}

	len, _ := transport.client.LLen(ctx, transport.cacheKeyID("")).Result()

	assert.Equal(t, int64(5), len)
}

func TestNewRedisTransport(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379?key_name=foo")
	transport, err := NewRedisTransport(u)
	assert.Nil(t, err)
	require.NotNil(t, transport)
	transport.Close()

	u, _ = url.Parse("redis:///127.0.0.1:6379")
	_, err = NewRedisTransport(u)
	assert.EqualError(t, err, "\"redis:///127.0.0.1:6379\": invalid \"redis\" dsn: invalid transport DSN")

	u, _ = url.Parse("redis://127.0.0.1:6379?size=invalid")
	_, err = NewRedisTransport(u)
	assert.EqualError(t, err, `"redis://127.0.0.1:6379?size=invalid": invalid "size" parameter "invalid": strconv.ParseUint: parsing "invalid": invalid syntax: invalid transport DSN`)
}

func TestRedisTransportDoNotDispatchedUntilListen(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	defer transport.Close()
	defer transport.Flush()
	assert.Implements(t, (*Transport)(nil), transport)

	s := NewSubscriber("", NewTopicSelectorStore())
	go s.start()
	require.Nil(t, transport.AddSubscriber(s))

	var (
		readUpdate *Update
		ok         bool
		wg         sync.WaitGroup
	)
	wg.Add(1)
	go func() {
		select {
		case readUpdate = <-s.Receive():
		case <-s.disconnected:
			ok = true
		}

		wg.Done()
	}()

	s.Disconnect()

	wg.Wait()
	assert.Nil(t, readUpdate)
	assert.True(t, ok)
}

func TestRedisTransportDispatch(t *testing.T) {
	ur, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(ur)
	defer transport.Close()
	defer transport.Flush()
	assert.Implements(t, (*Transport)(nil), transport)

	s := NewSubscriber("", NewTopicSelectorStore())
	s.Topics = []string{"https://example.com/foo"}
	go s.start()

	require.Nil(t, transport.AddSubscriber(s))

	u := &Update{Topics: s.Topics}
	require.Nil(t, transport.Dispatch(u))
	assert.Equal(t, u, <-s.Receive())
}

func TestRedisTransportClosed(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	require.NotNil(t, transport)
	defer transport.Close()
	defer transport.Flush()
	assert.Implements(t, (*Transport)(nil), transport)

	s := NewSubscriber("", NewTopicSelectorStore())
	s.Topics = []string{"https://example.com/foo"}
	go s.start()
	require.Nil(t, transport.AddSubscriber(s))

	require.Nil(t, transport.Close())
	require.NotNil(t, transport.AddSubscriber(s))

	assert.Equal(t, transport.Dispatch(&Update{Topics: s.Topics}), ErrClosedTransport)

	_, ok := <-s.disconnected
	assert.False(t, ok)
}

func TestRedisCleanDisconnectedSubscribers(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	require.NotNil(t, transport)
	defer transport.Close()
	defer transport.Flush()

	tss := NewTopicSelectorStore()

	s1 := NewSubscriber("", tss)
	go s1.start()
	require.Nil(t, transport.AddSubscriber(s1))

	s2 := NewSubscriber("", tss)
	go s2.start()
	require.Nil(t, transport.AddSubscriber(s2))

	assert.Len(t, transport.subscribers, 2)

	s1.Disconnect()
	assert.Len(t, transport.subscribers, 2)

	transport.Dispatch(&Update{Topics: s1.Topics})
	assert.Len(t, transport.subscribers, 1)

	s2.Disconnect()
	assert.Len(t, transport.subscribers, 1)

	transport.Dispatch(&Update{})
	assert.Len(t, transport.subscribers, 0)
}

func TestRedisGetSubscribers(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	require.NotNil(t, transport)
	defer transport.Close()
	defer transport.Flush()

	tss := NewTopicSelectorStore()

	s1 := NewSubscriber("", tss)
	go s1.start()
	require.Nil(t, transport.AddSubscriber(s1))

	s2 := NewSubscriber("", tss)
	go s2.start()
	require.Nil(t, transport.AddSubscriber(s2))

	lastEventID, subscribers := transport.GetSubscribers()
	assert.Equal(t, EarliestLastEventID, lastEventID)
	assert.Len(t, subscribers, 2)
	assert.Contains(t, subscribers, s1)
	assert.Contains(t, subscribers, s2)
}

func TestRedisLastEventID(t *testing.T) {
	redisOptions, _ := redis.ParseURL("redis://127.0.0.1:6379")
	var client = redis.NewClient(redisOptions)

	id, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: "mercure-hub-updates",
		Values: []string{"keya", "valueb"},
	}).Result()
	require.Nil(t, redisNilToNil(err))

	err = client.RPush(ctx, "mercure-hub-updates/bar", id).Err()
	require.Nil(t, redisNilToNil(err))

	client.Close()

	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport, _ := NewRedisTransport(u)
	require.NotNil(t, transport)
	defer transport.Close()
	defer transport.Flush()

	lastEventID, _ := transport.GetSubscribers()
	assert.Equal(t, id, lastEventID)
}

func TestRedisTransportScale(t *testing.T) {
	u, _ := url.Parse("redis://127.0.0.1:6379")
	transport1, _ := NewRedisTransport(u)
	transport2, _ := NewRedisTransport(u)
	defer transport1.Close()
	defer transport2.Close()
	defer transport1.Flush()

	topics := []string{"https://example.com/foo"}
	for i := 1; i <= 5; i++ {
		transport1.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	for i := 6; i <= 8; i++ {
		transport2.Dispatch(&Update{
			Event:  Event{ID: strconv.Itoa(i)},
			Topics: topics,
		})
	}

	s := NewSubscriber("2", NewTopicSelectorStore())
	s2 := NewSubscriber("6", NewTopicSelectorStore())
	s.Topics = topics
	s2.Topics = topics
	go s.start()
	go s2.start()

	require.Nil(t, transport1.AddSubscriber(s))
	require.Nil(t, transport2.AddSubscriber(s2))

	var count int
	for {
		u := <-s.Receive()
		// the reading loop must read the #3 ... #8 messages
		assert.Equal(t, strconv.Itoa(3+count), u.ID)
		count++
		if count == 5 {
			break
		}
	}

	var count2 int
	for {
		u := <-s2.Receive()
		// the reading loop must read the #7 and #8 messages
		assert.Equal(t, strconv.Itoa(7+count2), u.ID)
		count2++
		if count2 == 2 {
			break
		}
	}
}
