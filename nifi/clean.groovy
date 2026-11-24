import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.time.OffsetDateTime
import org.apache.nifi.processor.io.StreamCallback

def flow = session.get()
if (!flow) return
boolean valid = true
String messageKey = flow.getAttribute("kafka.key") ?: "invalid"
flow = session.write(flow, { input, output ->
    def original = input.getText("UTF-8")
    def result
    try {
        def record = new JsonSlurper().parseText(original)
        if (!(record instanceof Map)) throw new IllegalArgumentException("Expected a JSON object")
        def field = { name, limit ->
            def value = record[name]
            if (!(value instanceof String) || !value.trim() || value.trim().size() > limit)
                throw new IllegalArgumentException("Missing or invalid " + name)
            value.trim()
        }
        def number = { name, maximum ->
            def value = record[name]?.toString()?.trim()
            if (!(value ==~ /[0-9]{1,6}/) || value.toInteger() < 1 || value.toInteger() > maximum)
                throw new IllegalArgumentException("Invalid " + name)
            value.toInteger()
        }
        def kind = field("event_type", 16).toUpperCase(Locale.ROOT)
        if (!(kind in ["PLACED","PREPARING","READY","CLOCK","FINAL"]))
            throw new IllegalArgumentException("Unknown event_type")
        result = [event_id: field("event_id",96), session_id: field("session_id",36),
                  event_time: OffsetDateTime.parse(field("event_time",48)).toInstant().toString(), event_type: kind]
        messageKey = result.session_id
        if (kind in ["PLACED","PREPARING","READY"]) {
            def menu = new JsonSlurper().parse(new File("/menu/restaurant.json"))
            def stationId = field("station_id",24).toUpperCase(Locale.ROOT)
            def category = field("category",60)
            def name = field("item_name",80)
            def station = menu.stations.find { it.id == stationId }
            def item = station?.menu?.find { it.equalsIgnoreCase(name) }
            def sla = number("sla_seconds",999999)
            if (!item || !station.category.equalsIgnoreCase(category) || sla != station.sla_seconds)
                throw new IllegalArgumentException("Station, category, menu item or SLA mismatch")
            result += [station_id:stationId, category:station.category, item_name:item, sla_seconds:sla,
                       order_id:field("order_id",96), item_id:field("item_id",96), order_size:number("order_size",5)]
            messageKey = result.item_id
        }
    } catch (Exception error) {
        valid = false
        result = [reason:error.message, original:original]
    }
    output.write(JsonOutput.toJson(result).getBytes("UTF-8"))
} as StreamCallback)
flow = session.putAttribute(flow,"message.key",messageKey)
session.transfer(flow,valid ? REL_SUCCESS : REL_FAILURE)
