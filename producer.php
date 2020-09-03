<?php

require './vendor/autoload.php';
date_default_timezone_set('PRC');

// use Monolog\Logger;
// use Monolog\Handler\StdoutHandler;
use Kafka\Producer;
use Kafka\ProducerConfig;

// Create the logger
// $logger = new Logger('my_logger');

// // Now add some handlers
// $logger->pushHandler(new StdoutHandler());

$config = ProducerConfig::getInstance();
$config->setMetadataRefreshIntervalMs(10000);
$config->setMetadataBrokerList('192.168.0.157:9092');
$config->setBrokerVersion('1.0.0');
$config->setRequiredAck(1);
$config->setIsAsyn(false);
$config->setProduceInterval(500);

$topic = 'example-topic';

$producer = new Producer(
    function() use($topic) {
        return [
            [
                'topic' => $topic,
                'key' => 'testkey',
                'value' => '{"id":1,"name":"Adrian Crisan","address":"bb9 5sr"}',
                'timestamp' => time(),
            ],
        ];
    }
);
// $producer->setLogger($logger);
$producer->success(function($result) {
	var_dump($result);
});
$producer->error(function($errorCode) {
	var_dump($errorCode);
});
$producer->send(true);
