<?php

require './vendor/autoload.php';

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Messaging\Consumer\ConsumerException;
use \Jobcloud\Messaging\Kafka\Consumer\KafkaConsumerBuilder;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerEndOfPartitionException;
use Jobcloud\Messaging\Kafka\Exception\KafkaConsumerTimeoutException;
use Jobcloud\Messaging\Kafka\Message\Decoder\AvroDecoder;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistry;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\BlockingRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use GuzzleHttp\Client;

$cachedRegistry = new CachedRegistry(
    new BlockingRegistry(
        new PromisingRegistry(
            new Client(['base_uri' => 'localhost:8081'])
        )
    ),
    new AvroObjectCacheAdapter()
);

$topic = 'example-topic';
$group = 'example-group';

$registry = new AvroSchemaRegistry($cachedRegistry);
$recordSerializer = new RecordSerializer($cachedRegistry);

//if no version is defined, latest version will be used
//if no schema definition is defined, the appropriate version will be fetched form the registry
$registry->addSchemaMappingForTopic(
    $topic,
    new KafkaAvroSchema('example-topic-value' /*, 9, AvroSchema $definition */)
);

$decoder = new AvroDecoder($registry, $recordSerializer);

$consumer = KafkaConsumerBuilder::create()
     ->withAdditionalConfig(
        [
            'compression.codec' => 'lz4',
            'auto.commit.interval.ms' => 500
        ]
    )
    ->withDecoder($decoder)
    ->withAdditionalBroker('localhost:9092')
    ->withConsumerGroup($group)
    ->withTimeout(120 * 10000)
    ->withAdditionalSubscription($topic)
    ->build();

$consumer->subscribe();

while (true) {
    try {
        $message = $consumer->consume();
        // your business logic
        $consumer->commit($message);
    } catch (KafkaConsumerTimeoutException $e) {
        //no messages were read in a given time
    } catch (KafkaConsumerEndOfPartitionException $e) {
        //only occurs if enable.partition.eof is true (default: false)
    } catch (ConsumerException $e) {
        // Failed
    } 
}
