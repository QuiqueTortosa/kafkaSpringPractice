package com.toxxii.reactivekafkaplayground.sec03;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(producerConfig);

        var flux = Flux.range(1,10).map(KafkaProducer::createSenderRecord);

        var sender = KafkaSender.create(options);
        sender
                .send(flux) //key,value,resultType
                .doOnNext(result -> log.info("correlation id: {}", result.correlationMetadata())) //correlationMetadata es pr.key() del flux, esto se hace para ponerlo en el log etc para saber que eventos se enviaron correctamente
                .doOnComplete(sender::close)
                .subscribe();
    }

    private static SenderRecord<String,String,String> createSenderRecord(Integer i){
        var headers = new RecordHeaders();
        headers.add("client-id","some-client".getBytes());
        headers.add("tracingn-id","123".getBytes());
        var pr = new ProducerRecord<>("order-events",null,i.toString(),"order-"+i,headers);
        return  SenderRecord.create(pr,pr.key());
    }

}
