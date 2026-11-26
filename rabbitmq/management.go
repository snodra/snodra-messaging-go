package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"

	snodra "github.com/snodra/snodra-messaging-go"
)

// memberProperty is the subscribe property that tells the instances of one subscription apart
// (the management API shows it).
const memberProperty = "snodra-member"

// maxManagementAnswer is the largest answer read from the management API.
const maxManagementAnswer = 16 << 20

// consumer is one stream consumer the management API lists.
type consumer struct {
	name   string
	stream string
	member string
}

// statsEnabled asks the overview whether the metrics the consumer list comes from are collected.
// A management API that answers with its stats off (management.disable_stats or
// management_agent.disable_metrics_collector) cannot tell who reads a subscription.
func (m *management) statsEnabled(ctx context.Context) (bool, error) {
	var overview map[string]any
	if err := m.get(ctx, "overview", &overview); err != nil {
		return false, err
	}
	value, ok := overview["disable_stats"]
	if !ok {
		return true, nil
	}
	disabled, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%w: the RabbitMQ management API's overview: disable_stats is %v", snodra.ErrFailed, value)
	}
	return !disabled, nil
}

// consumers is every stream consumer of the virtual host.
func (m *management) consumers(ctx context.Context) ([]consumer, error) {
	var body any
	if err := m.get(ctx, "stream/consumers/"+url.PathEscape(m.vhost), &body); err != nil {
		return nil, err
	}
	return parseConsumers(body)
}

// members is the distinct members reading the subscription name on any of streams, sorted.
func (m *management) members(ctx context.Context, name string, streams []string) ([]string, error) {
	all, err := m.consumers(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var members []string
	for _, found := range all {
		if found.name != name {
			continue
		}
		known := false
		for _, stream := range streams {
			if found.stream == stream {
				known = true
				break
			}
		}
		if !known || seen[found.member] {
			continue
		}
		seen[found.member] = true
		members = append(members, found.member)
	}
	sort.Strings(members)
	return members, nil
}

// get reads one API answer as JSON.
func (m *management) get(ctx context.Context, path string, into any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, m.url+"/api/"+path, nil)
	if err != nil {
		return fmt.Errorf("%w: the RabbitMQ management API: %w", snodra.ErrFailed, err)
	}
	request.SetBasicAuth(m.username, m.password)
	response, err := m.client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: the RabbitMQ management API (%s): %w", snodra.ErrFailed, m.url, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("%w: the RabbitMQ management API (%s): HTTP %d", snodra.ErrFailed, m.url, response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxManagementAnswer))
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("%w: the RabbitMQ management API (%s): not JSON: %w", snodra.ErrFailed, m.url, err)
	}
	return nil
}

// parseConsumers reads the API's consumer list: an array (or a paged answer's `items`), each
// with queue.name and a properties object. A consumer without properties.name is an unnamed
// reader (a browser's, not a subscription's member).
func parseConsumers(body any) ([]consumer, error) {
	shape := func(what string) error {
		return fmt.Errorf("%w: the RabbitMQ management API's consumer list has an unexpected shape: %s", snodra.ErrFailed, what)
	}
	items, ok := body.([]any)
	if !ok {
		object, objectOK := body.(map[string]any)
		if !objectOK {
			return nil, shape("not an array of consumers")
		}
		items, ok = object["items"].([]any)
		if !ok {
			return nil, shape("not an array of consumers")
		}
	}
	all := make([]consumer, 0, len(items))
	for _, item := range items {
		entry, ok := item.(map[string]any)
		if !ok {
			return nil, shape("a consumer that is not an object")
		}
		queue, ok := entry["queue"].(map[string]any)
		if !ok {
			return nil, shape("a consumer without queue")
		}
		stream, ok := queue["name"].(string)
		if !ok {
			return nil, shape("a consumer without queue.name")
		}
		properties, ok := entry["properties"].(map[string]any)
		if !ok {
			return nil, shape("a consumer without properties")
		}
		name, err := text(properties["name"])
		if err != nil {
			return nil, err
		}
		member, err := text(properties[memberProperty])
		if err != nil {
			return nil, err
		}
		if member == "" {
			// Another client's consumer (the official Go client cannot set the property): its
			// connection and subscription id tell it apart.
			// TODO(spec-gap): SG-44 (a consumer without snodra-member is still a member).
			connection, _ := entry["connection_details"].(map[string]any)
			connectionName, _ := connection["name"].(string)
			member = fmt.Sprintf("%s:%v", connectionName, entry["subscription_id"])
		}
		all = append(all, consumer{name: name, stream: stream, member: member})
	}
	return all, nil
}

// text is an optional string property: absent is empty, another type is a malformed answer.
func text(value any) (string, error) {
	switch value := value.(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	default:
		return "", fmt.Errorf("%w: the RabbitMQ management API's consumer list has an unexpected shape: a property is %T, not a string", snodra.ErrFailed, value)
	}
}
