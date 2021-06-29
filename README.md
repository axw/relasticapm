# relasticapm

`relasticapm` extracts sampled transaction documents from Elasticsearch, and reconstitutes
them into their original Elastic APM ND-JSON event format suitable for sending to Elastic APM
Server.

For each transaction event a configurable command is executed. The command's stdin will be fed
with an ND-JSON event stream containing the agent metadata and the single event. The default
command executed is a `curl` command to send the transaction events to a local APM Server. The
server URL can be configured with `$ELASTIC_APM_SERVER_URL`. The command can be overridden
using the `-exec` flag, for example `-exec "jq ."`.

By default, events will be returned for the past hour. It is possible to specify an alternative
start time by passing `-since` with either a timestamp or Elasticsearch date math. Note that
`relasticapm` estimates the original sample rate by calculating the sampled-to-total transaction
ratio, so a short duration may lead to greater inaccuracy.

## Installation

`go get github.com/axw/relasticapm`

## Example

`relasticapm -es=httsp://user:password@elasticshearch.host:9200 -since=now-5m -exec=jq .`
