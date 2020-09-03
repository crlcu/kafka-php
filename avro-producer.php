<?php

require './vendor/autoload.php';

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use Jobcloud\Messaging\Kafka\Message\KafkaProducerMessage;
use Jobcloud\Messaging\Kafka\Message\Encoder\AvroEncoder;
use Jobcloud\Messaging\Kafka\Message\Registry\AvroSchemaRegistry;
use Jobcloud\Messaging\Kafka\Producer\KafkaProducerBuilder;
use Jobcloud\Messaging\Kafka\Message\KafkaAvroSchema;
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

$registry = new AvroSchemaRegistry($cachedRegistry);
$recordSerializer = new RecordSerializer($cachedRegistry);

$topic = 'customer';

//if no version is defined, latest version will be used
//if no schema definition is defined, the appropriate version will be fetched form the registry
$registry->addSchemaMappingForTopic(
    $topic,
    new KafkaAvroSchema('customer-value' /*, int $version, AvroSchema $definition */)
);

$encoder = new AvroEncoder($registry, $recordSerializer);

$producer = KafkaProducerBuilder::create()
    ->withAdditionalBroker('localhost:9092')
    ->withEncoder($encoder)
    ->build();

// $schemaName = 'testSchema';
// $version = 1;

$message = KafkaProducerMessage::create($topic, 0)
            ->withKey('testkey')
            ->withBody([
                'id' => 1,
                'name' => 'Adrian Crisan',
                'address' => 'bb9 5sr'
            ])
            ->withHeaders([ 'key' => 'value' ]);

$producer->produce($message);
