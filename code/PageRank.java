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

class PageRankResult implements Writable, Comparable, WritableComparable {
    public char node;
    public short inputLink;
    public short outputLink;
    public short pageRank;

    public void write(DataOutput out) throws IOException {
        out.writeShort(outputLink);
        out.writeShort(inputLink);
        out.writeShort(pageRank);
    }

    public void readFields(DataInput in) throws IOException {
        outputLink = in.readShort();
        inputLink = in.readShort();
        pageRank = in.readShort();
    }

    public static PageRankResult read(DataInput in) throws IOException {
        PageRankResult w = new PageRankResult();
        w.readFields(in);
        return w;
    }

    public String toString() {
        return String.valueOf(outputLink) + ' ' + String.valueOf(inputLink) + ' ' + String.valueOf(pageRank);
    }

    public int compareTo(Object o) {
        return String.valueOf(node).compareTo(String.valueOf(((PageRankResult) o).node));
    }
}

public class PageRank {

    //Mapper class
    public static class PageRankMapper extends MapReduceBase
            implements Mapper<LongWritable,/*Input key Type */
            Text,                /*Input value Type*/
            Text,                /*Output key Type*/
            PageRankResult>        /*Output value Type*/ {

        //Map function
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, PageRankResult> output,
                        Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer initialLine = new StringTokenizer(line, ":");
            String _tempLine;

            PageRankResult res = new PageRankResult();
            res.node = initialLine.nextToken().charAt(0);

            String[] nodesLine = initialLine.nextToken().split("-");

            //On récupère la ligne
            _tempLine = nodesLine[0];
            //On compte le nombre de lien entrant
            res.outputLink = (short) _tempLine.split(",").length;
            //Si = 1, il se peut que la ligne était en fait vide. Dans ce cas, on remet outputLink à zéro
            if (res.outputLink == 1 && _tempLine.equals(""))
                res.outputLink = 0;

            if (nodesLine.length == 2) {
                _tempLine = nodesLine[1];
                res.inputLink = (short) _tempLine.split(",").length;
                if (res.inputLink == 1 && _tempLine.equals(""))
                    res.inputLink = 0;
            } else {
                res.inputLink = 0;
            }

            res.pageRank = 0;

            output.collect(new Text(String.valueOf(res.node)), res);
        }
    }


    //Reducer class
    public static class PageRankReducer extends MapReduceBase implements
            Reducer<Text, PageRankResult, Text, PageRankResult> {

        //Reduce function
        public void reduce(Text key, Iterator<PageRankResult> values,
                           OutputCollector<Text, PageRankResult> output, Reporter reporter) throws IOException {
            PageRankResult val;

            while (values.hasNext()) {
                val = values.next();
                output.collect(key, val);
            }
        }
    }

    //Main function
    public static void main(String args[]) throws Exception {
        JobConf conf = new JobConf(PageRank.class);

        conf.setJobName("max_eletricityunits");

        conf.setOutputKeyClass(Text.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(PageRankResult.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(PageRankResult.class);

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
