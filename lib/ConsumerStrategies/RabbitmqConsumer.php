<?php
require_once(dirname(__FILE__) . "/AbstractConsumer.php");

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Consumes messages and writes them to a file
 */
class ConsumerStrategies_RabbitmqConsumer extends ConsumerStrategies_AbstractConsumer {

    /**
     * @var string path to a file that we want to write the messages to
     */
    private $_endpoint;


    /**
     * Creates a new FileConsumer and assigns properties from the $options array
     * @param array $options
     */
    function __construct($options) {
        parent::__construct($options);

        $this->_endpoint = str_replace('/', '', $options['endpoint']);
    }


    /**
     * Append $batch to a file
     * @param array $batch
     * @return bool
     */
    public function persist($batch) {
		$connection = new AMQPConnection('localhost', 5672, 'guest', 'guest');
		$channel = $connection->channel();

		$channel->queue_declare('webhooks', false, true, false, false);

		error_log(serialize($batch));
        $data = "data=" . $this->_encode($batch);
		$msg = new AMQPMessage($data, array('app_id' => 'mixpanel', 'type' => $this->_endpoint));
		$channel->basic_publish($msg, '', 'webhooks');

		$channel->close();
		$connection->close();

		return true;
    }
}
