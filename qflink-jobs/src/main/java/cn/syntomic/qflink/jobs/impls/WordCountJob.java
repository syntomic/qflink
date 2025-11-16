package cn.syntomic.qflink.jobs.impls;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import cn.syntomic.qflink.common.jobs.AbstractJob;

/** Word count demo */
public class WordCountJob extends AbstractJob {

    @Override
    public void run() throws Exception {
        DataStream<String> text =
                env.fromData(
                        new String[] {
                            "To be, or not to be,--that is the question:--",
                            "Whether 'tis nobler in the mind to suffer",
                            "The slings and arrows of outrageous fortune",
                            "Or to take arms against a sea of troubles,",
                            "And by opposing end them?--To die,--to sleep,--",
                            "No more; and by a sleep to say we end",
                            "The heartache, and the thousand natural shocks",
                            "That flesh is heir to,--'tis a consummation",
                            "Devoutly to be wish'd. To die,--to sleep;--",
                            "To sleep! perchance to dream:--ay, there's the rub;",
                            "For in that sleep of death what dreams may come,",
                            "When we have shuffled off this mortal coil,",
                            "Must give us pause: there's the respect",
                            "That makes calamity of so long life;",
                            "For who would bear the whips and scorns of time,",
                            "The oppressor's wrong, the proud man's contumely,",
                            "The pangs of despis'd love, the law's delay,",
                            "The insolence of office, and the spurns",
                            "That patient merit of the unworthy takes,",
                            "When he himself might his quietus make",
                            "With a bare bodkin? who would these fardels bear,",
                            "To grunt and sweat under a weary life,",
                            "But that the dread of something after death,--",
                            "The undiscover'd country, from whose bourn",
                            "No traveller returns,--puzzles the will,",
                            "And makes us rather bear those ills we have",
                            "Than fly to others that we know not of?",
                            "Thus conscience does make cowards of us all;",
                            "And thus the native hue of resolution",
                            "Is sicklied o'er with the pale cast of thought;",
                            "And enterprises of great pith and moment,",
                            "With this regard, their currents turn awry,",
                            "And lose the name of action.--Soft you now!",
                            "The fair Ophelia!--Nymph, in thy orisons",
                            "Be all my sins remember'd."
                        });

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(
                                new FlatMapFunction<String, Tuple2<String, Integer>>() {

                                    @Override
                                    public void flatMap(
                                            String value, Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        String[] tokens = value.toLowerCase().split("\\W+");
                                        for (String token : tokens) {
                                            if (token.length() > 0) {
                                                out.collect(new Tuple2<>(token, 1));
                                            }
                                        }
                                    }
                                })
                        .keyBy(value -> value.f0)
                        .sum(1);

        counts.print();

        env.execute("Word Count Job");
    }
}
