package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.datamodel.pheno.Sample;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.zip.GZIPInputStream;

public class ComputedSexLoad {
    
    private String version;
    private String dir;

    protected Logger log = LogManager.getLogger("csStatus");
    private final DAO dao = new DAO();
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    public void run() throws Exception {
        log.info(getVersion());
        log.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        log.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        File folder = new File(dir);
        ArrayList<File> files = new ArrayList<>();
        dao.listFilesInFolder(folder, files);

        for (File file : files) {
            // look at col[0] (gsm id) and col[2] (computed sex)
            log.info("\tParsing file: " +file.getName());
            List<Sample> updateSamples = new ArrayList<>();
            try(BufferedReader br = dao.openFile(file.getAbsolutePath())){
                String lineData = br.readLine(); // skips the header row
                while ((lineData = br.readLine()) != null){
                    String[] dataSplit = lineData.split("\t");
                    String gsmId = dataSplit[0];
                    String compSex = dataSplit[2];
                    Sample s = dao.getSampleByGeoSampleName(gsmId);
                    if (s==null) {
                        log.info("\t\tSample not fount for: "+gsmId);
                    }
                    else {
                        String sex = "";
                        switch (compSex.toLowerCase()){
                            case "male":
                            case "m":
                                sex="Male";
                                break;
                            case "female":
                            case "f":
                                sex="Female";
                                break;
                            default:
                                sex="Undetermined";
                                break;
                        }
                        if (!Utils.stringsAreEqualIgnoreCase(s.getComputedSex(),sex)) {
                            s.setComputedSex(sex);
                            updateSamples.add(s);
                        }
                    }
                } // end while
            }
            catch (Exception e){
                Utils.printStackTrace(e,log);
            }
            if (!updateSamples.isEmpty()){
                log.info("\tUpdating the computed sex for samples: "+updateSamples.size());
                dao.updateComputedSex(updateSamples);
            }

            log.info("===========================================================");
        }

        log.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");

    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setDir(String dir) {
        this.dir = dir;
    }

    public String getDir() {
        return dir;
    }
}
