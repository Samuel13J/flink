package com.jaywong.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author wangjie
 * @create 2023-04-23 15:46
 */
public class CountTrigger<T> extends Trigger<T, TimeWindow> {

//    当前的计数标志
    private static int flag = 0;
//    最大数量
    public static int threshold = 0;
    public CountTrigger(Integer threshold) {
        CountTrigger.threshold = threshold;
    }

    /**
     * 添加到窗口的每个元素都会调用此方法
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.registerEventTimeTimer(window.maxTimestamp());
        flag++;
        if (flag >= threshold) {
            flag = 0;
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     *  当注册的事件时间计时器触发时，将调用此方法
     */
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (flag > 0) {
            System.out.println("到达窗口时间执行触发: " + flag);
            flag = 0;
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }


    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (time >= window.maxTimestamp() && flag > 0) {
            System.out.println("到达时间窗口且有数据，触发操作！");
            flag = 0;
            return TriggerResult.FIRE_AND_PURGE;
        }else if (time >= window.maxTimestamp() && flag == 0) {
//            清除窗口但不触发
            return TriggerResult.PURGE;
        }
        return TriggerResult.CONTINUE;
    }

    /**
     *  执行任何需要清除的相应窗口
     */
    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }
}
