set -eu
for topic in kitchen.raw kitchen.cleaned kitchen.invalid; do
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists \
        --partitions 1 --replication-factor 1 --topic "$topic"
done
