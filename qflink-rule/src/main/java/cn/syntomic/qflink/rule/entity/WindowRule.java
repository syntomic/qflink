package cn.syntomic.qflink.rule.entity;

public class WindowRule {

    private WindowType type;
    private Trigger trigger;

    private long size;
    private long offset;
    private long step;

    public enum WindowType {
        TUMBLE,
        CUMULATE,
        SLIDE
    }

    public enum Trigger {
        BATCH,
        SINGLE,
        CONTINUOS,
        HYBRID
    }

    public WindowType getType() {
        return type;
    }

    public void setType(WindowType type) {
        this.type = type;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getStep() {
        return step;
    }

    public void setStep(long step) {
        this.step = step;
    }
}
