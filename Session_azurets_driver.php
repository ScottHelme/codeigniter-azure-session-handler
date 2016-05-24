<?php

use WindowsAzure\Common\ServicesBuilder;
use MicrosoftAzure\Storage\Table\TableRestProxy;
use MicrosoftAzure\Storage\Common\ServiceException;
use MicrosoftAzure\Storage\Table\Models\Entity;
use MicrosoftAzure\Storage\Table\Models\EdmType;
use MicrosoftAzure\Storage\Table\Models\BatchOperations;
use MicrosoftAzure\Storage\Table\Models\QueryEntitiesOptions;

class CI_Session_azurets_driver extends CI_Session_driver implements SessionHandlerInterface
{
    protected $tableRestProxy;
    protected $tableName = 'sessions';
    protected $partitionKey = 'session';
    protected $dataProperty = 'data';

    public function __construct(&$params)
    {
        parent::__construct($params);

        $ci =& get_instance();
        $ci->load->library('azure');
        $this->tableRestProxy = ServicesBuilder::getInstance()->createTableService($ci->config->item('azure_connection_string'));

        session_set_save_handler(
        array($this, 'open'),
        array($this, 'close'),
        array($this, 'read'),
        array($this, 'write'),
        array($this, 'destroy'),
        array($this, 'gc'));
    }

    public function __destruct()
    {
        session_write_close();
    }

    public function open($save_path, $session_id)
    {
        return true;
    }

    public function read($session_id)
    {
        try
        {
            $options = new QueryEntitiesOptions();
            $options->addSelectField($this->dataProperty);
            $result = $this->tableRestProxy->getEntity($this->tableName, $this->partitionKey, $session_id, $options);
            $entity = $result->getEntity();
            $data = $entity->getPropertyValue($this->dataProperty);
            return $data == null ? '' : unserialize(base64_decode($data));
        } catch(ServiceException $e)
        {
            return '';
        }
    }

    public function write($session_id, $session_data)
    {
        $entity = new Entity();
        $entity->setPartitionKey($this->partitionKey);
        $entity->setRowKey($session_id);
        $entity->addProperty($this->dataProperty, EdmType::STRING, base64_encode(serialize($session_data)));
        try
        {
            $this->tableRestProxy->insertOrReplaceEntity($this->tableName, $entity);
            return true;
        } catch(ServiceException $e)
        {
            return false;
        }
    }

    public function close()
    {
        return true;
    }

    public function destroy($session_id)
    {
        try
        {
            $this->tableRestProxy->deleteEntity($this->tableName, $this->partitionKey, $session_id);
            return true;
        } catch(ServiceException $e)
        {
            return false;
        }
    }

    public function gc($maxlifetime)
    {
        $maxAge = str_replace(" ", "T", date("Y-m-d H:i:s", time() - $maxlifetime));
        $filter = "PartitionKey eq '$this->partitionKey' and Timestamp lt datetime'$maxAge'";
        $options = new QueryEntitiesOptions();
        $options->setFilter(Filter::applyQueryString($filter));
        $options->addSelectField('RowKey');
        try
        {
            $result = $this->tableRestProxy->queryEntities($this->tableName, $options);
            $entities = $result->getEntities();
            $operations = new BatchOperations();
            $counter = 0;
            foreach($entities as $entity) {
                $operations->addDeleteEntity($this->tableName, $this->partitionKey, $entity->getRowKey());
                $counter += 1;
                if($counter == 100)
                {
                    try
                    {
                        $this->tableRestProxy->batch($operations);
                        $operations = new BatchOperations();
                        $counter = 0;
                    } catch(ServiceException $e)
                    {
                        return false;
                    }
                }
            }
            
            if($counter > 0)
            {
                try
                {
                    $this->tableRestProxy->batch($operations);
                } catch(ServiceException $e)
                {
                    return false;
                }
            }
            
            return true;
        } catch(ServiceException $e)
        {
            return false;
        }
    }
}

?>
