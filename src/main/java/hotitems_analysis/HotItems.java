package hotitems_analysis;

import hotitems_analysis.bean.ItemViewCount;
import hotitems_analysis.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;

public class HotItems {
    public static void main(String[] args) throws Exception {

        //1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //每 1000ms 开始一次 checkpoint
        //检查点和保存点的区别是
        //检查点是flink自动定时周期性的保存，恢复数据(需要在代码中开启)
        //保存点是手动保存，恢复数据，用于有计划的手动备份、更新应用程序、版本迁移、暂停、重启应用等
        env.enableCheckpointing(1000);

        //2. 读取数据
        DataStreamSource<String> inputStream = env.readTextFile("D:\\javaProject\\flinkDemo\\src\\main\\resources\\UserBehavior.csv");


        //3. 转化为POJO，分配事件时间和watermark
        SingleOutputStreamOperator<UserBehavior> dataStream = inputStream.map(line -> {

            String[] fields = line.split(",");


            return new UserBehavior(Long.valueOf(fields[0]), Long.valueOf(fields[1]),
                    Integer.valueOf(fields[2]), fields[3], Long.valueOf(fields[4]));
        })

                //设置水位线，事件处理时间，延时等待20s
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                                .withTimestampAssigner((event, timestamp) ->
                                        //秒转化为毫秒,返回的是毫秒
                                        event.getTimestamp() * 1000L));

        // 4. 分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<ItemViewCount> windowAggStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior()))   //过滤pv行为
                .keyBy(UserBehavior::getItemId)   //按商品id分组
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))) // 开滑动窗口
                //watermark和allowedLateness区别
                //1. watermark 通过additional的时间戳来控制窗口激活的时间，主要是为了解决数据乱序到达的问题，
                //2. allowedLateness 用来控制窗口的销毁时间，解决窗口触发后数据迟到后的问题
                //3. 在flink中我们经常使用watermark、allowedLateness 、 sideOutputLateData结合的方式处理数据保证窗口数据不丢失
                .allowedLateness(Time.minutes(1))
                .aggregate(new ItemCountAgg(), new WindowItemCountResult());


        //5. 收集同一窗口的所有商品count数据，排序输入topN
        SingleOutputStreamOperator<String> resultStream = windowAggStream.keyBy(ItemViewCount::getWindowEnd).process(new TopNHotItems(5));

//        //数据输出到外部中间件
//        final StreamingFileSink<String> fileSink = StreamingFileSink
//                .forRowFormat(new Path("D:\\javaProject\\flinkDemo\\src\\main\\resources\\out"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                                .withMaxPartSize(1024 * 1024 * 1024)
//                                .build())
//                .build();
//        resultStream.addSink(fileSink);

        resultStream.print();

        //打印执行计划
        System.out.println("执行计划");
        System.out.println(env.getExecutionPlan());
        env.execute("热度商品排名");

    }


    //实现自定义增量聚合函数
    //接口的三个参数 输入类型、累加器类型、输出类型
    //这里是输入，UserBehavior，累加器是Long，输出的是同一个商品的总数，Long
    //窗口函数
    public static class ItemCountAgg implements AggregateFunction<UserBehavior, Long, Long> {

        //初始化累加器
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        /**
         * @param userBehavior 当前数据
         * @param aLong        上一次累加器的结果
         * @return
         */
        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        /**
         * 获取结果
         *
         * @param aLong 累加器
         * @return
         */
        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        /**
         * 不同的分区
         *
         * @param aLong 分区a的结果
         * @param acc1  分区b的结果
         * @return
         */
        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    /**
     * 自定义全窗口函数
     * ProcessWindowFunction 获得一个包含窗口所有元素的 Iterable
     * 和一个可以访问时间和状态信息的 Context 对象，这使其能够提供比其他窗口函数更大的灵活性。
     * 这是以性能和资源消耗为代价的，因为元素不能增量聚合，
     * 而是需要在内部缓冲，直到窗口被认为准备好进行处理
     * <p>
     * 四个泛型
     * 输入的类型
     * 输出的类型
     * 分组key的类型
     * 窗口
     */
    public static class WindowItemCountResult extends ProcessWindowFunction<Long, ItemViewCount, Long, TimeWindow> {

        /**
         * @param itemId  分组key
         * @param context 窗口的上下文
         * @param input   被求值窗口中的元素。
         * @param out
         * @throws Exception
         */
        @Override
        public void process(Long itemId, Context context, Iterable<Long> input, Collector<ItemViewCount> out) {

            //窗口结束的时间
            Long windowEnd = context.window().getEnd();

            Long count = input.iterator().next();

            out.collect(new ItemViewCount(itemId, windowEnd, count));

        }
    }


    //过程函数
    //实现自定义的KeyedProcessFunction
    public static class TopNHotItems extends KeyedProcessFunction<Long, ItemViewCount, String> {

        // 定义属性，top n的大小
        private Integer topSize;

        // 定义列表状态，保存当前窗口内所有输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //初始化状态list
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<>("item-view-count-list", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context context, Collector<String> collector) throws Exception {
          // 每来一条数据，存入List中，并注册定时器
            itemViewCountListState.add(itemViewCount);
            //定时器的id相同，不会重复注册
            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1);
        }

        //定时器触发
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);



            // 定时器触发，当前已收集到所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());

            //排序
            itemViewCounts.sort(((o1, o2) ->  o2.getCount().intValue() - o1.getCount().intValue()));

            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===================================\n");
            resultBuilder.append("窗口结束时间：").append( new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取top n输出
            for( int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++ ){
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("===============================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());


            //销毁资源
            //1. 关闭定时器
            ctx.timerService().deleteEventTimeTimer(timestamp);
            //2. 清空数据,迟到数据，会重新触发该key的处理函数
            itemViewCountListState.clear();
        }
    }
}
