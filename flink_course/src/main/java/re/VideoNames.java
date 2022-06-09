package re;

import java.io.File;
import java.io.FilenameFilter;

public class VideoNames {

    public static void main(String[] args) {

        File file = new File("e:/");
        File[] subDirs = file.listFiles();
        for (File subDir : subDirs) {
            String subDirPath = subDir.getAbsolutePath();
            if(subDir.getName().startsWith("doit30")){
                String day = subDirPath.split("-")[2];
                File[] mp4s = subDir.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith("mp4");
                    }
                });
                for (File mp4File : mp4s) {
                    mp4File.renameTo(new File(subDirPath+"/"+day+"_"+mp4File.getName()));
                }


            }
        }

    }
}
