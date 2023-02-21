# Apache Beam
- 배치 및 데이터 스트림 병렬 처리 파이프라인을 정의하는 unified model
- 정의된 파이프라인은 Pipe runner(분산 처리 백엔드 - Apache Flink, Apache Spark, Google Cloud Dataflow 등)에 의해 실행된다.

### 어디에 사용하나?
- 작은 데이터 묶음들을 병렬로 처리하기
- 데이터를 다른 시스템에 옮기기
- 데이터 포맷 변경하기

## Java SDK
[프로그래밍 가이드](https://beam.apache.org/documentation/programming-guide/)
- `Pipeline`: 전체 데이터 프로세싱 과정(read-process-write)
- `PCollection`: 분산된 데이터셋, 파이프라인의 input이기도 하고 output이기도 하다.
    - bounded: 고정적 파일
    - unbounded: 지속적으로 업데이트 되는 소스 (e.g. 다른 시스템을 sub하는 경우)
- `PTransform`: 데이터 프로세싱 로직, PCollection을 받아 작업을 처리한다.

### 일반적인 Beam driver program의 워크 플로우
1. 파이프라인 옵션 지정
2. `Pipeline` 생성
3. 첫 `PCollection` 생성 - 외부 시스템 or 인메모리 데이터
4. 각 `PCollection`에 `PTransform` 적용
5. 마지막 `PCollection` 산출
6. 이렇게 만든 파이프라인을 지정한 파이프러너에서 실행한다!

### PTransform
```
[Output PCollection] = [Input Collection].apply([Transform])
```
- PCollection은 Immutable하므로 여러 개의 transform을 하나의 인풋 PCollection에 apply 할 수 있다.

### Core Beam PTransform
- `ParDo`
    - 병렬 처리
    - MapReduce와 비슷한 개념
    - `DoFn`(프로세싱 로직)을 제공해야 한다.
    ```
    PCollection<Integer> wordLengths = words.apply(ParDo.of(new ComputeWordLengthFn()));
    ```
- `GroupByKey`
    - 키/밸류 데이터 처리
    - 같은 키를 가진 모든 밸류들을 모은다.
- `CoGroupByKey`
    - 키/밸류 데이터 처리
    - 같은 키를 통해 relational join을 수행한다.
- `Combine`
- `Flatten`
    - 같은 데이터 타입을 가지는 데이터들에 대해, 데이터를 합친다(merge).
- `Partition`
    - 같은 데이터 타입을 가지는 데이터들에 대해, 하나의 PCollection을 여러개의 작은 PCollection들로 나눈다.

### DoFn
프로세싱 로직을 정의한다. 이게 코드 작성시 제일 중요한 부분이다.
병렬로 진행된다는 점을 유의해야 한다. state를 공유하지 말아야 한다.
- Requirements
    - Your function object must be *seriallizable*.
    - Your function object must be thread-compatible, and be aware that the Beam SDKs are *not thread-safe*.
- `@ProcessElement` 메서드에 프로세싱 로직을 정의한다. `@Element` 인풋을 받아 `OutputReceiver`에 아웃풋을 전달한다.

### Windowing
unbounded 데이터에 대해 `GroupByKey`나 `Combine` 같은 Transform을 실행할 때 PCollection을 timestamp에 따라 나눈다. unbounded 데이터는 언제 끝날지 모르는, 계속 생산되는 데이터이기 때문이다.
- 기본 Windowing 동작은 모든 데이터를 하나의 글로벌 Window에 배정하고 이후의 데이터는 버린다.
- unbounded 데이터에 대해 그룹핑 Transform을 실행하기 전에 해야할 일:
    - Set a non-global windowing function.
    - Set a non-global trigger.

### Trigger
데이터를 여러개의 Window들로 나누는 경우, _언제_ 각 Window의 아웃풋을 내보낼지 결정한다.
- Event time triggers
- Processing time triggers
- Data-driven triggers
- Composite triggers

### Metrics
파이프라인 진행 상태에 대한 인사이트를 제공한다.
- `Counter`
- `Distribution`
- `Guage`