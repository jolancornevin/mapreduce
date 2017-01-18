package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;

/**
 * Function used in the map to properly parse each line of the file in PageRankStruct
 */
class MapParseur {
    //The given node
    public String node;
    //All ouput nodes
    public Vector<String> outputLink;
    //The actual pageRank score
    public double pageRank;

    /**
     * Set the outputLink from an array of string
     *
     * @param s
     */
    public void setOutputLink(String[] s) {
        outputLink = new Vector<String>();
        if (s != null) {
            for (String value : s) {
                outputLink.add(value.trim());
            }
        }
    }

    /**
     * For a given line from the input file, parse datas and set them in attributs
     *
     * @param str
     */
    public void setFromLine(String str) {
        StringTokenizer initialLine = new StringTokenizer(str, ":");

        this.node = initialLine.nextToken().trim();
        //Get the next token
        String token = initialLine.nextToken();

        //Here, we try if we can cast the token to a double.
        // If so, then the node had not output links. Else, we get them and then get the page rank
        try {
            this.pageRank = Double.valueOf(token);
        } catch (NumberFormatException e) {
            this.setOutputLink(token.split(","));
        }

        if (this.outputLink != null)
            this.pageRank = Double.valueOf(initialLine.nextToken());
        else
            this.outputLink = new Vector<String>();
    }
}

/**
 * Function used in two ways :
 * META : nodes: Contains all outputlinks of a given node (the node itself is passed through the key)
 * This is usefull to reconstruct the input file in order to iterate on all datas
 * DATA : score: How much of our PageRank we give to an other node (the node itself is passed through the key)
 */
class PageRankStruct implements Writable, Comparable, WritableComparable {
    //Vector of all nodes
    public Vector<String> nodes;
    public double score;
    public PageRankState state;

    public PageRankStruct() {
        nodes = new Vector<String>();
        state = PageRankState.NULL;
    }

    public PageRankStruct(Vector<String> nodes, double givenPageRank, PageRankState state) {
        setNodes(nodes);
        this.score = givenPageRank;
        this.state = state;
    }

    public void setNodes(Vector<String> s) {
        nodes = new Vector<String>();
        if (s != null) {
            for (String value : s)
                nodes.add(value.trim());
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(nodes.size());

        for (String aNode : nodes) {
            out.writeBytes(aNode.trim() + "\n");
        }

        out.writeDouble(score);
        out.writeInt(state.ordinal());
    }

    public void readFields(DataInput in) throws IOException {
        int totalNodes = in.readInt();
        nodes = new Vector<String>();

        for (short iNode = 0; iNode < totalNodes; ++iNode) {
            String val = in.readLine();
            nodes.add(val);
        }

        score = in.readDouble();
        state = PageRankState.values()[in.readInt()];
    }

    public String toString() {
        String str = "";

        if (nodes.size() > 0) {
            for (short iChar = 0; iChar < nodes.size() - 1; ++iChar)
                str += nodes.get(iChar) + ",";

            str += nodes.get(nodes.size() - 1);
        }

        return ":" + str + ":" + String.valueOf(score);
    }

    public int compareTo(Object o) {
        return String.valueOf(nodes).compareTo(String.valueOf(((MapParseur) o).node));
    }
}

enum PageRankState {
    META,
    DATA,
    NULL;
}

/**
 * Class Sort, containing the map and reduce functions for sorting our PageRank output
 * The actual sort is done by configuring our job with conf.setOutputKeyComparatorClass(DoubleWritable.Comparator.class);
 */
class Sort {
    //For a given line, we parse it and output the pageRank as the key and the node as the value
    public static class SortMapper extends MapReduceBase
            implements Mapper<LongWritable,     /*Input key Type */
            Text,                           /*Input value Type*/
            DoubleWritable,                 /*Output key Type*/
            Text>                           /*Output value Type*/ {
        public void map(LongWritable key, Text value, OutputCollector<DoubleWritable, Text> output, Reporter reporter)
                throws IOException {
            MapParseur res = new MapParseur();

            res.setFromLine(value.toString());

            output.collect(new DoubleWritable(res.pageRank), new Text(res.node));
        }
    }

    //Reducer class
    public static class SortReducer extends MapReduceBase implements Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        //Reduce function
        public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<DoubleWritable, Text> output, Reporter reporter)
                throws IOException {
            Text val;

            while (values.hasNext()) {
                val = values.next();
                output.collect(key, val);
            }
        }
    }
}

/**
 * Class PageRank doing the actually job.
 */
public class PageRank {
    //Mapper class
    public static class PageRankMapper extends MapReduceBase
            implements Mapper<LongWritable,/*Input key Type */
            Text,                /*Input value Type*/
            Text,                /*Output key Type*/
            PageRankStruct>        /*Output value Type*/ {

        //Map function
        public void map(LongWritable key, Text value, OutputCollector<Text, PageRankStruct> output, Reporter reporter)
                throws IOException {
            MapParseur res = new MapParseur();
            res.setFromLine(value.toString());

            //Send the output nodes of the given to the reduce function,
            // in the aim to be able to reconstruct the input file later as we are going to iterate on it
            output.collect(new Text(String.valueOf(res.node)), new PageRankStruct(res.outputLink, 0, PageRankState.META));

            //For each output node, we send it some of our pagerank score
            for (String node : res.outputLink) {
                output.collect(new Text(String.valueOf(node)), new PageRankStruct(new Vector<String>(), 0.85 * (res.pageRank / res.outputLink.size()), PageRankState.DATA));
            }
        }
    }

    //Reducer class
    public static class PageRankReducer extends MapReduceBase implements Reducer<Text, PageRankStruct, Text, PageRankStruct> {
        //Reduce function
        public void reduce(Text key, Iterator<PageRankStruct> values, OutputCollector<Text, PageRankStruct> output, Reporter reporter)
                throws IOException {
            PageRankStruct out = new PageRankStruct();
            PageRankStruct val;
            out.score = 0.15;

            while (values.hasNext()) {
                val = values.next();
                if (val.state == PageRankState.META)
                    out.setNodes(val.nodes);
                else if (val.state == PageRankState.DATA)
                    out.score += val.score;
            }

            output.collect(key, out);
        }
    }

    //Main function
    public static void main(String args[]) throws Exception {
        int iterations = new Integer(args[2]);

        Path inPath = new Path(args[0]);
        Path outPath = null;
        JobConf conf = null;

        //Iterate on our pageRank algorithm, in the aim to calculate the correct score.
        int i = 0;
        for (; i < iterations; ++i) {
            outPath = new Path(args[1] + i);

            conf = new JobConf(PageRank.class);
            conf.setJobName("mapreducepagerank");

            conf.setOutputKeyClass(Text.class);
            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(PageRankStruct.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(PageRankStruct.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            conf.setMapperClass(PageRankMapper.class);
            conf.setReducerClass(PageRankReducer.class);

            FileInputFormat.addInputPath(conf, inPath);
            FileOutputFormat.setOutputPath(conf, outPath);

            JobClient.runJob(conf);

            inPath = outPath;
        }

        // Now, we are going to sort our pageRank score
        outPath = new Path(args[1] + i);

        conf = new JobConf(Sort.class);
        conf.setJobName("sorting values");

        conf.setOutputKeyClass(Text.class);

        conf.setMapOutputKeyClass(DoubleWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(DoubleWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyComparatorClass(DoubleWritable.Comparator.class);

        conf.setMapperClass(Sort.SortMapper.class);
        conf.setReducerClass(Sort.SortReducer.class);

        FileInputFormat.addInputPath(conf, inPath);
        FileOutputFormat.setOutputPath(conf, outPath);

        JobClient.runJob(conf);
    }
}
