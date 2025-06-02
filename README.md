# WiFi Analyzer - Unisem

WiFi 센서로부터 수집한 정보를 통합적으로 분석하기 위한 실시간 데이터 처리 시스템입니다. Scala와 Akka를 기반으로 개발되었으며, Kafka를 통한 스트림 데이터 처리와 MySQL/MariaDB를 이용한 데이터 저장 및 분석 기능을 제공합니다.

## 📋 목차

- [프로젝트 개요](#프로젝트-개요)
- [시스템 아키텍처](#시스템-아키텍처)
- [기술 스택](#기술-스택)
- [프로젝트 구조](#프로젝트-구조)
- [설치 및 실행](#설치-및-실행)
- [설정](#설정)
- [데이터 모델](#데이터-모델)
- [API 및 메시지](#api-및-메시지)
- [모니터링 및 로깅](#모니터링-및-로깅)

## 🎯 프로젝트 개요

이 프로젝트는 WiFi 센서 네트워크에서 수집되는 실시간 데이터를 처리하고 분석하는 시스템입니다. 주요 기능은 다음과 같습니다:

- **실시간 데이터 수집**: Kafka를 통한 WiFi 센서 데이터 스트림 처리
- **병렬 데이터 처리**: Akka Actor 모델을 활용한 고성능 병렬 처리
- **데이터 분석**: 역별 통계 및 이동 흐름 분석
- **데이터 저장**: MySQL/MariaDB 기반 데이터 웨어하우스
- **스케줄링**: Quartz 기반 배치 작업 처리

## 🏗️ 시스템 아키텍처

```
WiFi 센서 → Kafka → Consumer Actors → 데이터 처리 → 데이터베이스
                      ↓
                  분석 엔진 → 통계 생성 → 리포트
```

### 주요 컴포넌트

1. **Kafka Consumer**: 실시간 WiFi 센서 데이터 수집
2. **Consumer Actors**: 멀티스레드 데이터 처리 (RoundRobin 방식)
3. **Log Process Actors**: 디바이스 로그 데이터 처리
4. **Database Layer**: 이중 데이터베이스 구조 (분석 DB + 로그 DB)
5. **Scheduler**: 일간/월간 통계 생성 및 데이터 파티셔닝

## 🛠️ 기술 스택

- **언어**: Scala 2.12.6
- **프레임워크**: Akka HTTP 10.1.0, Akka Stream 2.5.11
- **메시지 큐**: Apache Kafka 1.0.0
- **데이터베이스**: MySQL/MariaDB
- **연결 풀**: HikariCP 2.7.5
- **스케줄러**: Quartz 2.2.1
- **JSON 처리**: Circe 0.9.3
- **로깅**: Logback + SLF4J
- **빌드 도구**: SBT 1.1.5
- **캐시**: Redis (Jedis 2.9.0)

## 📁 프로젝트 구조

```
src/main/scala/com/unisem/metrobus/analyzer/
├── MetrobusAnalyzerMain.scala          # 애플리케이션 진입점
├── ConsumerContainer.scala             # Kafka Consumer 컨테이너
├── ConsumerActor.scala                 # 메인 데이터 처리 액터
├── LogProcessActor.scala               # 로그 처리 액터
├── InstanceChecker.scala               # 인스턴스 중복 실행 방지
├── common/
│   └── DBRecords.scala                 # 데이터베이스 레코드 정의
├── db/
│   ├── DBDataSource.scala              # 메인 데이터베이스 연결
│   └── DeviceLogDBSource.scala         # 디바이스 로그 DB 연결
├── messages/
│   └── Messages.scala                  # Actor 메시지 정의
├── scheduler/
│   └── DailyScheduleActor.scala        # 일간 스케줄 액터
└── cronjob/
    └── CronJobScheduler.scala          # Cron 작업 스케줄러
```

## 🚀 설치 및 실행

### 사전 요구사항

- Java 8+
- SBT 1.1.5+
- Apache Kafka
- MySQL/MariaDB
- Redis (선택사항)

### Kafka 설정

```bash
# 1. Zookeeper 시작
bin/zookeeper-server-start.sh config/zookeeper.properties

# 2. Kafka 서버 시작
bin/kafka-server-start.sh config/server.properties

# 3. 토픽 생성
bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 5 --topic "topic-analyzer-trial"

bin/kafka-topics.sh --create --zookeeper localhost:2181 \
  --replication-factor 1 --partitions 3 --topic "topic-device-log-trial"
```

### 애플리케이션 빌드 및 실행

```bash
# 빌드
sbt compile

# JAR 파일 생성
sbt assembly

# 실행
java -Dconfig.file=application.conf \
     -Xmx1g \
     -Dlogback.configurationFile=./logback.xml \
     -cp ./metrobus-analyzer.jar \
     com.unisem.metrobus.analyzer.MetrobusAnalyzerMain
```

## ⚙️ 설정

### application.conf 주요 설정

```hocon
analyzer {
  consumer {
    analyzer_topic = "topic-analyzer-trial"
    analyzer_group = "group-analyzer-trial"
    analyzer_broker_info = "localhost:9092"
    consumer_count = 10
    device_logger_count = 5
  }
  
  DB {
    db_conn = "jdbc:mysql://localhost:3306/metro_istanbul"
    db_name = "metro_istanbul"
    db_id = "root"
    db_pwd = "password"
    db_max_pool = 20
  }
  
  timezone_delta = 0
}
```

### 환경별 설정 파일

- `application.conf`: 로컬 개발 환경
- `analyzer-cloud.conf`: 클라우드 프로덕션 환경

## 📊 데이터 모델

### WiFi 센서 데이터

```scala
case class DeviceRecord(
  sensorID: String,     // 센서 고유 ID
  sensorType: String,   // 센서 타입
  stationID: String,    // 역 ID
  deviceID: String,     // 디바이스 ID
  power: Int,           // 신호 강도
  dsStatus: Int,        // 디바이스 상태
  datetime: String      // 수집 시간
)
```

### 통계 데이터

```scala
case class StationStatsMonthlyRecord(
  station_id: String,
  stat_date: String,
  total_count: Long
)

case class FlowsStatsMonthlyRecord(
  src_station_id: String,
  dst_station_id: String,
  stat_date: String,
  total_count: Long
)
```

## 📨 API 및 메시지

### Actor 메시지

- `StartConsumer`: Consumer 시작
- `ConsumeRecord`: 레코드 처리
- `ProcessRecords`: 배치 레코드 처리
- `MakeTablePartition`: 테이블 파티션 생성
- `ShutDownConsumer`: Consumer 종료

### 데이터 흐름

1. **Kafka Consumer** → WiFi 센서 데이터 수신
2. **ConsumerActor** → 데이터 파싱 및 검증
3. **Database Writer** → 실시간 데이터 저장
4. **Scheduler** → 주기적 통계 생성
5. **Partition Manager** → 데이터 파티셔닝

## 📈 모니터링 및 로깅

### 로깅 설정

- **Logback**: 구조화된 로깅
- **SLF4J**: 로깅 추상화 레이어
- **로그 레벨**: DEBUG, INFO, WARN, ERROR

### 모니터링 포인트

- Kafka Consumer Lag
- Actor Mailbox 크기
- 데이터베이스 연결 풀 상태
- 메모리 사용량
- 처리 처리량 (TPS)

### 인스턴스 관리

- **중복 실행 방지**: `/tmp/lock_analyzer` 파일 기반 락
- **Graceful Shutdown**: Actor 시스템 안전 종료
- **리소스 정리**: DB 연결, Kafka Consumer 정리

## 🔧 개발 환경

### IDE 설정

IntelliJ IDEA 또는 Visual Studio Code 사용 권장

### 로컬 개발

```bash
# 의존성 확인
sbt update

# 테스트 실행
sbt test

# 코드 포맷팅
sbt scalafmt
```

## 📞 지원

- **개발자**: Heung-Mook CHOI (zorba)

---

**참고**: 이 시스템은 실시간 WiFi 센서 데이터 처리를 위해 설계되었으며, 높은 처리량과 안정성을 제공합니다.
