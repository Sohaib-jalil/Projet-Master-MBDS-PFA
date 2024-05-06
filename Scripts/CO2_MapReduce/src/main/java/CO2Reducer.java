import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class CO2Reducer extends Reducer<Text, Text, Text, Text> {
    private static final DecimalFormat df = new DecimalFormat("0.00");

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double totalBonusMalus = 0;
        int countBonusMalus = 0;
        int count = 0;
        double totalRejetCo2 = 0;
        double totalCout = 0;

        if (key.toString().equals("TotalBonusMalus")) {
            for (Text row : values) {
                String[] columns = row.toString().split(",");
                if (TotalBonusMalus.total < Double.parseDouble(columns[0])) {
                    // store the value in a class variable to use later
                    TotalBonusMalus.total = Double.parseDouble(columns[0]);
                    TotalBonusMalus.count = Integer.parseInt(columns[1]);
                }
            }
        } else {
            for (Text row : values) {
                String[] columns = row.toString().split(",");

                if (!columns[1].equals("") && !columns[1].equals("0")) {
                    totalBonusMalus += Double.parseDouble(columns[1]);
                    countBonusMalus++;
                }

                if (totalBonusMalus == 0) {
                    totalBonusMalus = TotalBonusMalus.total;
                    countBonusMalus = TotalBonusMalus.count;
                }

                totalRejetCo2 += Double.parseDouble(columns[2]);

                // Remove non-numeric characters before parsing totalCout
                String numericPart = columns[3].replaceAll("[^0-9.]", "");
                totalCout += Double.parseDouble(numericPart);
                count++;
            }

            // Check if countBonusMalus is zero to avoid division by zero
            double averageBonusMalus = (countBonusMalus != 0) ? totalBonusMalus / countBonusMalus : 0.0;
            double averageRejetCo2 = totalRejetCo2 / count;
            double averageCout = totalCout / count;

            context.write(key, new Text(df.format(averageBonusMalus) + "," + df.format(averageRejetCo2) + "," + df.format(averageCout)));
        }
    }
}
