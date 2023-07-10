package experiment.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import java.io.IOException;


public class Q10 {

    public static void main(String[] args) throws IOException {

        Path parquetFilePath = new Path(args[0] + "parquet/data.parquet");
        String word = args[1]; //word
        float budget1 = Float.parseFloat(args[2]); //budget
        float budget2 = Float.parseFloat(args[3]);
        float pid = Long.parseLong(args[4]); // p_id

        // 创建 ParquetReader.Builder 实例
        ParquetReader.Builder<Group> builder = ParquetReader.builder(new GroupReadSupport(), parquetFilePath);
        // 创建 ParquetReader 实例
        ParquetReader<Group> reader = builder.build();
        // 循环读取 Parquet 文件中的记录
        Group record;
        int ansNum = 0;
        int sum = 0;
        long t1 = System.currentTimeMillis();
        while ((record = reader.read()) != null) {
            sum++;
            Group campaignList = record.getGroup("CampaignList", 0);
            try {
                int budgetIndex = 0;
                Group budgetGroup;
                while(true){
                    budgetGroup = campaignList.getGroup("list", budgetIndex++).getGroup("item", 0);
                    double budget = budgetGroup.getDouble("c_budget", 0);
                    if (budget1 > budget || budget2 <= budget){
                        break;
                    }
                }

            }catch (Exception e){
                continue;
            }

            Group wordSetList;
            Group wordList;
            Group wordItem;
            Group clickList;
            Group pcList;
            try{
                int wordSetIndex = 0;
                while (true){
                    wordSetList = campaignList.getGroup("item", wordSetIndex++);
                    wordSetList = wordSetList.getGroup("WordSetList", 0).getGroup("list", 0);
                    try{
                        int wordIndex = 0;
                        while (true){
                            wordList = wordSetList.getGroup("item", wordIndex++).getGroup("WordList", 0).getGroup("list", 0);
                            try {
                                int wordidx = 0;
                                while (true){
                                    wordItem = wordList.getGroup("item", wordidx++);
                                    String nowWord = wordItem.getString("wo_word", 0);
                                    if (nowWord.compareTo(word) == 0){
                                        break;
                                    }
                                }
                                break;
                            }catch (Exception e){
                                continue;
                            }

                        }
                        break;
                    }catch (Exception e){
                        continue;
                    }

                }
            }catch (Exception e){
                continue;
            }
            try{
                int clickIndex = 0;
                while (true){
                    clickList = campaignList.getGroup("item", clickIndex++);
                    clickList = clickList.getGroup("ClickList", 0).getGroup("list", 0);
                    try {
                        int pcIndex = 0;
                        while (true){
                            pcList = clickList.getGroup("item", pcIndex++);
                            long nowPid = pcList.getLong("p_id", 0);
                            if (nowPid == pid){
                                ansNum++;
                                break;
                            }
                        }
                        break;
                    }catch (Exception e){
                        continue;
                    }
                }

            }catch (Exception e){
                continue;
            }

        }
        long t2 = System.currentTimeMillis();
        System.out.println("filter result:" + ansNum);
        System.out.println("all data num:" + sum);
        System.out.println("time: " + (t2 - t1));
        // 关闭读取器
        reader.close();

    }

}
