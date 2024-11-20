package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecord;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecordValue;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionValueCount;
import edu.mcw.rgd.datamodel.pheno.Sample;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.FileSystemResource;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Main {
    private String version;
    private String file;
    private int speciesType;
    private int mapKey;
    protected Logger logger = LogManager.getLogger("status");
    private final DAO dao = new DAO();
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    public static void main(String[] args) throws Exception {
        DefaultListableBeanFactory bf = new DefaultListableBeanFactory();
        new XmlBeanDefinitionReader(bf).loadBeanDefinitions(new FileSystemResource("properties/AppConfigure.xml"));
        try {
            Main main = (Main) bf.getBean("main");
            main.run(args);
        }
        catch (Exception e) {
            Utils.printStackTrace(e, LogManager.getLogger("status"));
            throw e;
        }
    }

    void run(String[] args) throws Exception {
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        Map<Integer, GeneExpressionRecord> recordMap = new HashMap<>();
        Map<Integer,String> cmoMap = new HashMap<>();
        List<GeneExpressionRecordValue> values = new ArrayList<>();
//        System.out.println("Working Directory = " + System.getProperty("user.dir"));
//        Map<Integer,>
        try (BufferedReader br = openFile(System.getProperty("user.dir")+file)) {
            String lineData;
            int i = 0;
            while ((lineData = br.readLine()) != null) {
                String[] parsedLine = lineData.split("\t");
                if (i==0){// row 0 is GEO sample names
                    int j = 0;
                    for (String col : parsedLine){
                        if (j>0) {
                            String sampleName = "";
                            String[] parseCol = col.split("\\.");
                            if (!parseCol[0].startsWith("G"))
                                sampleName = parseCol[0].substring(1);
                            else
                                sampleName = parseCol[0];
//                            System.out.println(sampleName);
                            Sample s = dao.getSampleByGeoSampleName(sampleName);
                            if (s==null){
                                logger.info("\t"+sampleName+" does not exist!");
                                recordMap.put(j,null);
                                cmoMap.put(j,null);
                            }
                            else{
                                GeneExpressionRecord r = dao.getGeneExpressionRecordBySample(s.getId());
                                recordMap.put(j, r);
                                cmoMap.put(j,s.getCellTypeAccId());
                            }
                        }
                        j++;
                    }
                } // end first row
                else {
                    String geneSymbol = parsedLine[0];
                    geneSymbol = geneSymbol.replace("\"","");
                    Gene gene = dao.getGeneBySymbol(geneSymbol.toLowerCase(),speciesType);

                    for (int j = 1; j < parsedLine.length; j++){
                        if (gene==null){
                            logger.info("\tGene:"+geneSymbol+" was not found!");
                            break;
                        }
                        if (recordMap.get(j)==null){ // there is no record
                            continue;
                        }
                        double value = Double.parseDouble(parsedLine[j]);
//                        if (value==0)
//                            continue;
                        /* TPM Levels
                         * High          x > 1000
                         * Medium        10 < x <= 1000
                         * Low           .5 <= x <= 10
                         * Below Cutoff  x < .5
                         */
                        GeneExpressionRecordValue v = new GeneExpressionRecordValue();
                        v.setMapKey(mapKey);
                        v.setExpressedObjectRgdId(gene.getRgdId());
                        v.setExpressionUnit("TPM");
                        v.setTpmValue(value);
                        v.setExpressionValue(value);
                        v.setGeneExpressionRecordId(recordMap.get(j).getId());
                        v.setExpressionMeasurementAccId(cmoMap.get(j));
                        if (value > 1000)
                            v.setExpressionLevel("high");
                        else if (value <= 1000 && value > 10)
                            v.setExpressionLevel("medium");
                        else if (value <= 10 && value >= .5)
                            v.setExpressionLevel("low");
                        else
                            v.setExpressionLevel("below cutoff");
                        if (!checkValueExists(v))
                            values.add(v);
                    }
                }

                i++;
            }
            if (!values.isEmpty()){
                logger.info("\t\tExpression Values being entered: "+values.size());
                dao.insertExpressionRecordValues(values);
            }
        }
        catch (Exception e){
            Utils.printStackTrace(e,logger);
        }

        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");
    }

    boolean checkValueExists(GeneExpressionRecordValue incomingValue) throws Exception{
        List<GeneExpressionRecordValue> existingValues = dao.getGeneExpressionRecordValuesByRecordIdAndExpressedObjId(incomingValue.getGeneExpressionRecordId(),incomingValue.getExpressedObjectRgdId(),"TPM");
        if (existingValues.isEmpty())
            return false;
        for (GeneExpressionRecordValue v : existingValues){
            if (Double.compare(v.getExpressionValue(),incomingValue.getExpressionValue()) == 0 &&
                    Utils.stringsAreEqual(v.getExpressionMeasurementAccId(),incomingValue.getExpressionMeasurementAccId()) )
                return true;
        }
        return false;
    }

    private BufferedReader openFile(String fileName) throws IOException {

        String encoding = "UTF-8"; // default encoding

        InputStream is;
        if( fileName.endsWith(".gz") ) {
            is = new GZIPInputStream(new FileInputStream(fileName));
        } else {
            is = new FileInputStream(fileName);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is, encoding));
        return reader;
    }

    public void setVersion(String version) {
        this.version=version;
    }

    public String getVersion() {
        return version;
    }

    public void setFile(String file) {
        this.file = file;
    }

    public String getFile() {
        return file;
    }

    public void setSpeciesType(int speciesType) {
        this.speciesType = speciesType;
    }

    public int getSpeciesType() {
        return speciesType;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public int getMapKey() {
        return mapKey;
    }
}