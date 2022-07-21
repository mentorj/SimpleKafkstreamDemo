package com.javaxpert.demos.lbp.streams;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

/**
 * tests suite containing unit tests (not integration ones)
 * using TopologyTestDriver class.
 * These tests should be used in conjonction with integration tests in containers  & embedded kafka
 * to ensure asynchronous & caching are OK!!
 * @author jerome@javaxpert.com
 */
public class SimpleStreamTestsSuite {
    private TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    private final Logger logger = LoggerFactory.getLogger(SimpleStreamTestsSuite.class);
    private Properties config = new Properties();

    @BeforeEach
    public void setupUnitTest() {
        config = Utils.mkProperties(Utils.mkMap(
                Utils.mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestDemoStreamingProcess"),
                Utils.mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"),
                Utils.mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.Long().getClass().getName()),
                Utils.mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName())
        ));
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("in")
                .filter((k, v) -> {
                    boolean contained = v.toString().contains("test");
                    System.out.println("Input msg = " + v + " contains test pattern? " + contained );
                    return  contained;})

                .to("out");
        testDriver = new TopologyTestDriver(builder.build(), config);
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    public void messageWithoutPatternShouldNotBeSentToOutputTopic(){
        TestInputTopic<Long,String> testInputTopic = testDriver.createInputTopic("in",longSerde.serializer(),stringSerde.serializer());
        TestOutputTopic<Long,String> testOutputTopic = testDriver.createOutputTopic("out",longSerde.deserializer(),stringSerde.deserializer());

        Assertions.assertTrue(testOutputTopic.isEmpty());
        testInputTopic.pipeInput(1L,"no pattern");
        Assertions.assertTrue(testOutputTopic.isEmpty());
        //Assertions.assertTrue(testOutputTopic.readValue().equals("no pattern"));
        //System.out.println("Value read from out topic is ="+  testOutputTopic.readValue());
    }


    @Test
    public void messageWithTestPatternShouldBeSentToOutputTopic(){
        TestInputTopic<Long,String> testInputTopic = testDriver.createInputTopic("in",longSerde.serializer(),stringSerde.serializer());
        TestOutputTopic<Long,String> testOutputTopic = testDriver.createOutputTopic("out",longSerde.deserializer(),stringSerde.deserializer());

        testInputTopic.pipeInput("test pattern is present");
        Assertions.assertTrue(testOutputTopic.readValue().equals("test pattern is present"));
    }

}
