import { Controller, Get, Query } from '@nestjs/common';
import { AppService } from './app.service';
import { Client } from '@elastic/elasticsearch';
import * as mysql from 'mysql2/promise';
import { Consumer, Kafka, Producer } from 'kafkajs';
import { createClient, RedisClientType } from 'redis';
import { ApiPropertyOptional } from '@nestjs/swagger';

class TestServicesQuery {
  @ApiPropertyOptional({
    default: '["http://localhost:9200"]',
  })
  elasticNode?: string;
  @ApiPropertyOptional()
  elasticUser?: string;
  @ApiPropertyOptional()
  elasticPass?: string;
  @ApiPropertyOptional({
    default: 'localhost',
  })
  mysqlHost?: string;
  @ApiPropertyOptional({
    default: '3306',
  })
  mysqlPort?: string;
  @ApiPropertyOptional()
  mysqlUser?: string;
  @ApiPropertyOptional()
  mysqlPass?: string;
  @ApiPropertyOptional()
  mysqlDb?: string;

  @ApiPropertyOptional({
    default: '["localhost:9092"]',
  })
  kafkaBrokers?: string;
  @ApiPropertyOptional({ default: 'redis://localhost:6379' })
  redisUrl?: string;
  @ApiPropertyOptional()
  redisUser?: string;
  @ApiPropertyOptional()
  redisPass?: string;
}
@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  @Get()
  async getHello(@Query() query: TestServicesQuery) {
    const result = {
      elasticSearch: {
        status: true,
        msg: null,
      },
      redis: { status: true, msg: null },
      kafka: { status: true, msg: null },
      mysql: { status: true, msg: null },
    };
    // ELASTICSEARCH
    let elasticClient: Client;
    try {
      elasticClient = new Client({
        nodes: JSON.parse(query.elasticNode),
        ...(query.elasticUser && query.elasticPass
          ? {
              auth: {
                username: query.elasticUser,
                password: query.elasticPass,
              },
            }
          : {}),
      });
      await elasticClient.cluster.health();
    } catch (ex) {
      result.elasticSearch = {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await elasticClient?.close?.();
    }

    // MYSQL
    let connection: mysql.Connection;
    try {
      connection = await mysql.createConnection({
        host: query.mysqlHost,
        port: parseInt(query.mysqlPort),
        user: query.mysqlUser,
        password: query.mysqlPass,
        database: query.mysqlDb,
      });

      await connection.connect();
    } catch (ex) {
      result.mysql = {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await connection?.end?.();
    }

    // KAFKA
    let kafka: Kafka;
    let producer: Producer;
    let consumer: Consumer;
    try {
      kafka = new Kafka({
        clientId: 'my-app',
        brokers: JSON.parse(query.kafkaBrokers),
      });

      producer = kafka.producer();
      consumer = kafka.consumer({ groupId: 'test-group' });
      await producer.connect();
      await producer.send({
        topic: 'test-topic',
        messages: [{ value: 'Hello KafkaJS user!' }],
      });

      // Consuming
      await consumer.connect();
      await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
      await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          console.log({
            partition,
            offset: message.offset,
            value: message.value.toString(),
          });
        },
      });
    } catch (ex) {
      result.kafka = {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await consumer?.disconnect?.();
      await producer?.disconnect?.();
    }

    //REDIS

    let redisClient: RedisClientType;
    try {
      redisClient = createClient({
        url: query.redisUrl,
        ...(query.redisPass && query.redisUser
          ? {
              username: query.redisUser,
              password: query.redisPass,
            }
          : {}),
      });
      redisClient.on('error', (err) => console.log('Redis Client Error', err));
      await redisClient.connect();
      const result = await redisClient.ping();
      console.log(result);
    } catch (ex) {
      result.redis = {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await redisClient?.disconnect?.();
    }
    return result;
  }
}
