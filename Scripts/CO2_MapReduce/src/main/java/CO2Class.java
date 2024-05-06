import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CO2Class {

    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        String[] params=new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job=Job.getInstance(conf, "CO2_Job");

        job.setJarByClass(CO2Class.class);
        job.setMapperClass(CO2Mapper.class);
        job.setReducerClass(CO2Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(params[0]));
        FileOutputFormat.setOutputPath(job, new Path(params[1]));

        if(job.waitForCompletion(true))
            System.exit(0);
        System.exit(-1);
    }
}
