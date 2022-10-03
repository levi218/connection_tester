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
  @ApiPropertyOptional({
    default: 'user',
  })
  mysqlUser?: string;
  @ApiPropertyOptional({
    default: 'secret',
  })
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
const withTimeout = (millis, promise) => {
  const timeout = new Promise((resolve, reject) =>
    setTimeout(() => resolve({ status: false, msg: 'Timed out' }), millis),
  );
  return Promise.race([promise, timeout]);
};

@Controller()
export class AppController {
  constructor(private readonly appService: AppService) {}

  async testElasticSearch(query: TestServicesQuery) {
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
      return {
        status: true,
        msg: null,
      };
    } catch (ex) {
      return {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await elasticClient?.close?.();
    }
  }
  async testMysql(query: TestServicesQuery) {
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
      return {
        status: true,
        msg: null,
      };
    } catch (ex) {
      return {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await connection?.end?.();
    }
  }
  async testKafka(query: TestServicesQuery) {
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
      return {
        status: true,
        msg: null,
      };
    } catch (ex) {
      return {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await consumer?.disconnect?.();
      await producer?.disconnect?.();
    }
  }
  async testRedis(query: TestServicesQuery) {
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
      return {
        status: true,
        msg: null,
      };
    } catch (ex) {
      return {
        status: false,
        msg: ex.toString() + '\n\n' + ex?.stack,
      };
    } finally {
      await redisClient?.disconnect?.();
    }
  }
  @Get()
  async getHello(@Query() query: TestServicesQuery) {
    const tests = await Promise.allSettled([
      withTimeout(60000, this.testElasticSearch(query)),
      withTimeout(60000, this.testRedis(query)),
      withTimeout(60000, this.testKafka(query)),
      withTimeout(60000, this.testMysql(query)),
    ]);
    const result = {
      elasticSearch: tests[0],
      redis: tests[1],
      kafka: tests[2],
      mysql: tests[3],
    };
    return result;
  }
}
