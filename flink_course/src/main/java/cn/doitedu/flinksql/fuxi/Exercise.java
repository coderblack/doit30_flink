package cn.doitedu.flinksql.fuxi;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;

public class Exercise {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);


        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/checkpoint");

        // ??????????????????
        env.setStateBackend(new HashMapStateBackend()); // ?????????????????????
        env.setStateBackend(new EmbeddedRocksDBStateBackend()); // rocksdb????????????

        // ????????????????????????
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setTopics("doit30-2")
                .setGroupId("gp01")
                .setBootstrapServers("doitedu:9092")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("auto.offset.commit", "true")
                .build();


        // ??? source????????? pipeline ???StreamGraph???
        DataStream<String> stream1 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kfk", TypeInformation.of(String.class));

        // ??????map???????????????????????????json??????????????????javabean??????

        SingleOutputStreamOperator<EventBean> stream2 = stream1.map(new MapFunction<String, EventBean>() {
            @Override
            public EventBean map(String value) throws Exception {
                return JSON.parseObject(value, EventBean.class);
            }
        });


        // ??????watermark????????????
        SingleOutputStreamOperator<EventBean> stream3 = stream2.assignTimestampsAndWatermarks(WatermarkStrategy
                .<EventBean>forBoundedOutOfOrderness(Duration.ofMillis(2000))
                .withTimestampAssigner(new SerializableTimestampAssigner<EventBean>() {
                    @Override
                    public long extractTimestamp(EventBean element, long recordTimestamp) {
                        return element.getEventTs();
                    }
                }));


        // ?????? ?????????????????? APP???????????????- UV
        KeyedStream<EventBean, String> keyedStream = stream3.keyBy(new KeySelector<EventBean, String>() {
            @Override
            public String getKey(EventBean value) throws Exception {
                return "0";
            }
        });

        SingleOutputStreamOperator<Integer> resultStream = keyedStream.process(new KeyedProcessFunction<String, EventBean, Integer>() {
            ValueState<RoaringBitmap> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // ????????????  ????????????????????? ???????????? roaringBitMap?????? ???
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<RoaringBitmap>("dictinct", RoaringBitmap.class));
            }

            @Override
            public void processElement(EventBean eventBean, KeyedProcessFunction<String, EventBean, Integer>.Context ctx, Collector<Integer> out) throws Exception {

                RoaringBitmap bitMap = valueState.value();
                if (bitMap == null) {
                    bitMap = RoaringBitmap.bitmapOf();
                }

                // ??????????????????????????? guid???????????? bitmap?????????
                bitMap.add((int) eventBean.getGuid());

                // ???????????????bitmap????????????????????????????????????
                valueState.update(bitMap);

                // ???????????????????????? ???????????????
                out.collect(bitMap.getCardinality());
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });


        // ??????????????????????????????????????????-PV
        SingleOutputStreamOperator<EventBean> filtered = stream3.filter(new FilterFunction<EventBean>() {
            @Override
            public boolean filter(EventBean value) throws Exception {
                return value.getEventId().equals("pageload");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream2 = filtered
                .map(bean -> Tuple2.of(bean.getProperties().get("pageId"), 1)).returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(tp -> tp.f0)
                .sum(1);


        /* resultStream.print();*/
        resultStream2.print();


        // ???5???????????????????????????10???????????????????????????????????????????????????

        // reduce/ sum ??????????????????
        stream3.filter(bean -> bean.getEventId().equals("addcart"))
                .map(bean -> Tuple2.of(bean.getProperties().get("pageId"), 1)).returns(new TypeHint<Tuple2<String, Integer>>() {
                })
                .keyBy(tp -> tp.f0)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))
                /*.sum(1)*/
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + 1);
                    }
                });

        // aggregate ??????????????????
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream4 =
                stream3.filter(bean -> bean.getEventId().equals("addcart"))
                        .keyBy(bean -> bean.getProperties().get("pageId"))
                        .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))
                        .aggregate(new AggregateFunction<EventBean, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> createAccumulator() {
                                return Tuple2.of("", 0);
                            }

                            @Override
                            public Tuple2<String, Integer> add(EventBean value, Tuple2<String, Integer> accumulator) {

                                return Tuple2.of(value.getProperties().get("pageId"), accumulator.f1 + 1);
                            }

                            @Override
                            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                                return accumulator;
                            }

                            /*
                              ??????????????????????????????
                             */
                            @Override
                            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                                return Tuple2.of(a.f0, a.f1 + b.f1);
                            }
                        });



        SingleOutputStreamOperator<String> resultStream5 =
                stream3.filter(bean -> bean.getEventId().equals("addcart"))
                        .keyBy(bean -> bean.getProperties().get("pageId"))
                        .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.minutes(5)))
                        .process(new ProcessWindowFunction<EventBean, String, String, TimeWindow>() {
                            @Override
                            public void process(String key, ProcessWindowFunction<EventBean, String, String, TimeWindow>.Context context, Iterable<EventBean> elements, Collector<String> out) throws Exception {

                                int count = 0;
                                for (EventBean element : elements) {
                                    count++;
                                }

                                TimeWindow window = context.window();
                                out.collect(window.getStart() +" : " + window.getEnd() + "," + key + " => " + count);

                            }
                        });

        resultStream4.print();

        env.execute();

    }
}
