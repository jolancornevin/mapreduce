import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

class PageRankStruct implements Writable, Comparable, WritableComparable {
    public char node;
    public short totalOutLinks;
    public Vector<Character> outputLink;
    public double pageRank;

    public void write(DataOutput out) throws IOException {
        out.writeShort(totalOutLinks);
        for (short iChar = 0; iChar < totalOutLinks; ++iChar)
            out.writeChar(outputLink.get(iChar));
        out.writeDouble(pageRank);
    }

    public void readFields(DataInput in) throws IOException {
        totalOutLinks = in.readShort();
        for (short iChar = 0; iChar < totalOutLinks; ++iChar)
            outputLink.add(in.readChar());
        pageRank = in.readDouble();
    }

    public static PageRankStruct read(DataInput in) throws IOException {
        PageRankStruct w = new PageRankStruct();
        w.readFields(in);
        return w;
    }

    public void setOutputLink(String s) {
        if (s == null) {
            outputLink = new Vector<Character>();
        } else {
            int len = s.length();
            Vector<Character> array = new Vector<Character>();
            for (int i = 0; i < len; i++) {
                array.add(s.charAt(i));
            }

            outputLink = array;
        }
    }

    public void setOutputLink(String[] s) {
        if (s == null) {
            outputLink = new Vector<Character>();
        } else {
            int len = s.length;
            Vector<Character> array = new Vector<Character>();
            for (String value : s) {
                array.add(value.charAt(0));
            }

            outputLink = array;
        }
    }

    public String toString() {
        return String.valueOf(outputLink) + ' ' + String.valueOf(totalOutLinks) + ' ' + String.valueOf(pageRank);
    }

    public int compareTo(Object o) {
        return String.valueOf(node).compareTo(String.valueOf(((PageRankStruct) o).node));
    }
}

class OutMapFunctionStruct implements Writable, Comparable, WritableComparable {
    public char targetNode;
    public double givenPageRank;

    public OutMapFunctionStruct() {

    }

    public OutMapFunctionStruct(char targetNode, double givenPageRank) {
        this.targetNode = targetNode;
        this.givenPageRank = givenPageRank;
    }

    public void write(DataOutput out) throws IOException {
        out.writeChar(targetNode);
        out.writeDouble(givenPageRank);
    }

    public void readFields(DataInput in) throws IOException {
        targetNode = in.readChar();
        givenPageRank = in.readDouble();
    }

    public static PageRankStruct read(DataInput in) throws IOException {
        PageRankStruct w = new PageRankStruct();
        w.readFields(in);
        return w;
    }

    public String toString() {
        return String.valueOf(targetNode) + ' ' + String.valueOf(givenPageRank);
    }

    public int compareTo(Object o) {
        return String.valueOf(targetNode).compareTo(String.valueOf(((PageRankStruct) o).node));
    }
}

public class PageRank {
    //Mapper class
    public static class PageRankMapper extends MapReduceBase
            implements Mapper<LongWritable,/*Input key Type */
            Text,                /*Input value Type*/
            Text,                /*Output key Type*/
            OutMapFunctionStruct>        /*Output value Type*/ {

        //Map function
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, OutMapFunctionStruct> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer initialLine = new StringTokenizer(line, ":");

            PageRankStruct res = new PageRankStruct();
            res.node = initialLine.nextToken().charAt(0);

            //On compte le nombre de lien entrant
            res.setOutputLink(initialLine.nextToken().split(","));

            res.pageRank = Short.valueOf(initialLine.nextToken());

            for (Character node : res.outputLink) {
                output.collect(new Text(String.valueOf(res.node)), new OutMapFunctionStruct(node, 0.85 * (res.pageRank / res.outputLink.size())));
            }
        }
    }


    //Reducer class
    public static class PageRankReducer extends MapReduceBase implements
            Reducer<Text, OutMapFunctionStruct, Text, OutMapFunctionStruct> {

        //Reduce function
        public void reduce(Text key, Iterator<OutMapFunctionStruct> values,
                           OutputCollector<Text, OutMapFunctionStruct> output, Reporter reporter) throws IOException {
            OutMapFunctionStruct val;

            while (values.hasNext()) {
                val = values.next();
                output.collect(key, val);
            }
        }
    }

    //Main function
    public static void main(String args[]) throws Exception {
        JobConf conf = new JobConf(PageRank.class);

        conf.setJobName("mapreducepagerank");

        conf.setOutputKeyClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(OutMapFunctionStruct.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(OutMapFunctionStruct.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setMapperClass(PageRankMapper.class);
        conf.setCombinerClass(PageRankReducer.class);
        conf.setReducerClass(PageRankReducer.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
