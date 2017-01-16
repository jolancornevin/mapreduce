package hadoop;

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

/**
 * Function used in the map function to properly parse each line of the file in PageRankStruct
 */
class MapParseur implements Writable, Comparable, WritableComparable {
    public String node;
    public short totalOutLinks;
    public Vector<String> outputLink;
    public double pageRank;

    public void write(DataOutput out) throws IOException {
        out.writeShort(totalOutLinks);
        for (short iStr = 0; iStr < totalOutLinks; ++iStr)
            out.writeChars(outputLink.get(iStr).trim());
        out.writeDouble(pageRank);
    }

    public void readFields(DataInput in) throws IOException {
        totalOutLinks = in.readShort();
        for (short iChar = 0; iChar < totalOutLinks; ++iChar)
            outputLink.add(in.readLine().trim());
        pageRank = in.readDouble();
    }

    public static MapParseur read(DataInput in) throws IOException {
        MapParseur w = new MapParseur();
        w.readFields(in);
        return w;
    }

    public void setOutputLink(String[] s) {
        outputLink = new Vector<String>();
        if (s != null) {
            for (String value : s) {
                outputLink.add(value.trim());
            }
        }
    }

    public String toString() {
        return String.valueOf(outputLink) + " " + String.valueOf(totalOutLinks) + " " + String.valueOf(pageRank);
    }

    public int compareTo(Object o) {
        return String.valueOf(node).compareTo(String.valueOf(((MapParseur) o).node));
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
            out.writeChars(aNode.trim() + "\n");
        }

        out.writeDouble(score);
        out.writeInt(state.ordinal());
    }

    public void readFields(DataInput in) throws IOException {
        int totalNodes = in.readInt();
        nodes = new Vector<String>();

        for (short iNode = 0; iNode < totalNodes; ++iNode) {
            nodes.add(in.readLine().trim());
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
            StringTokenizer initialLine = new StringTokenizer(value.toString(), ":");

            MapParseur res = new MapParseur();
            res.node = initialLine.nextToken().trim();

            //Get all nodes pointed by res.node
            res.setOutputLink(initialLine.nextToken().split(","));

            //Get the actual score of res.node
            res.pageRank = Double.valueOf(initialLine.nextToken());

            //Send pointed nodes to the reduce function, in the aim to be able to reconstruct the input file later
            output.collect(new Text(String.valueOf(res.node)), new PageRankStruct(res.outputLink, 0, PageRankState.META));

            //For each nodes, we send some of our pagerank score
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
                //If the state is META, then we get the pointed node back
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

        for (int i = 0; i < iterations; ++i) {
            outPath = new Path(args[1] + i);

            JobConf conf = new JobConf(PageRank.class);
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
    }
}
