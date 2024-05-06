import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CO2Mapper extends Mapper<Object, Text, Text, Text> {

    private static final Text Total_KEY = new Text("TotalBonusMalus");
    private static final Text FOR_ALL_KEY = new Text("forAll");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String row = value.toString();
        String[] columns = row.split(",");

        if (columns[0].isEmpty()) {
            return; // Skip the first row
        }

        // Extract marque and clean it
        String[] carInfo = columns[1].split("\\s+");
        String marque = columns[1].split("\\s+")[0];

        int initialeIndex;
        if (marque.equals("\"KIA")){
            initialeIndex = 1;
        } else {
            initialeIndex = 0;
        }

        marque = marque.replaceAll("[^a-zA-Z0-9]", "");

        // Extract and clean newBonusMalus
        String newBonusMalus = columns[initialeIndex+2].replaceAll("^-", "");
        newBonusMalus = newBonusMalus.replaceAll("1$", "");
        newBonusMalus = newBonusMalus.replaceAll("[^a-zA-Z0-9]", "");
        if (newBonusMalus.isEmpty()) {
            newBonusMalus = "0";
        }

        // If newBonusMalus is not zero, update the total and count
        if (!newBonusMalus.equals("0")) {
            try {
                double bonusMalusValue = Double.parseDouble(newBonusMalus);
                TotalBonusMalus.total += bonusMalusValue;
                TotalBonusMalus.count += 1;
            } catch (NumberFormatException e) {
                // Log the error or handle it appropriately
                System.err.println("Failed to parse bonus malus value: " + newBonusMalus);
            }

        }

        // Prepare output value
        String outputValue = marque + "," + newBonusMalus + "," + columns[initialeIndex+3] + "," + columns[initialeIndex+4];

        // Prepare output total
        String outputTotal = String.valueOf(TotalBonusMalus.total) + "," + String.valueOf(TotalBonusMalus.count);

        // Write output total for 'TotalBonusMalus' key
        context.write(Total_KEY, new Text(outputTotal));

        // Write output value for 'marque' key
        context.write(new Text(marque), new Text(outputValue));

        // Write output value for 'forAll' key
        context.write(FOR_ALL_KEY, new Text(outputValue));
    }
}
