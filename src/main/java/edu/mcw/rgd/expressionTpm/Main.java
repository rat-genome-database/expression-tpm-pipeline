package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.datamodel.Transcript;
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
//    private String file;
    private String genesFile;
    private String isoformFile;
    private String gtfFile;
    private String study;
    private int speciesType;
    private int mapKey;
    protected Logger logger = LogManager.getLogger("status");
    protected Logger notFoundLog = LogManager.getLogger("notFoundStatus");
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

        logger.info("Running for study: " + study);

        boolean isGenes = true;
        Map<Integer, GeneExpressionRecord> recordMap = new HashMap<>();
        Map<Integer,String> cmoMap = new HashMap<>();
        List<GeneExpressionRecordValue> values = new ArrayList<>();
        Map<String, Gene> geneMap = new HashMap<>();
        for (String arg : args){
            isGenes = switch (arg) {
                case "-genes" -> true;
                case "-isoforms" -> false;
                default -> true;
            };
        }
        String file = "";
        if (isGenes) {
            file = study + genesFile;
            geneMap = getGenesFromGTF();
        }
        else
            file = study+isoformFile;
        try (BufferedReader br = openFile("data/"+file)) {
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
                    String symbol = parsedLine[0];;
                    String geneSymbol = "";
                    Gene gene = null;
                    Transcript transcript = null;
                    if (isGenes) {
                        geneSymbol = symbol.replace("\"", "");
                        if (geneSymbol.startsWith("RGD")) {
                            int rgdId = Integer.parseInt(geneSymbol.replace("RGD",""));
                            gene = dao.getGeneByRgdId(rgdId);
                        }
                        else {
                            gene = geneMap.get(geneSymbol);
//                            if (genes.size()>1){
//                                logger.info("\tGene: "+geneSymbol+" has multiple based on symbol...");
//                                for (Gene g : genes){
//                                    logger.info("\t\tGene Symbol: "+g.getSymbol()+" | Gene RGD ID: "+g.getRgdId());
//                                    List<MapData> maps = dao.getMapDataByRgdIdAndMapKey(g.getRgdId(),mapKey);
//                                    if (maps == null || maps.isEmpty())
//                                        maps = dao.getMapDataByRgdIdAndMapKey(g.getRgdId(),373);
//                                    for (MapData md : maps){
//                                        logger.info("\t\t"+md.dump("|"));
//                                    }
//                                }
//                                continue;
//                            }
//                            else if (genes.size()==1)
//                                gene=genes.get(0);
                        }

                    }
                    else{
                        // get transcript data somehow
                        // remove trailing decimal and find transcript
                        symbol = symbol.replace("\"", "");
                        int trimIndex = symbol.indexOf(".");
                        String acc = "";
                        if (trimIndex<0)
                            acc = symbol;
                        else
                            acc = symbol.substring(0,trimIndex);
                        List<Transcript> tList = dao.getTranscriptsByAccId(acc);
                        if (tList.size()==1){
                            transcript = tList.get(0);
                        }
                        else if (tList.size()>1){
                            logger.info("\tTranscript: "+acc+" has multiple records...");
                            for (Transcript t : tList){
                                logger.info("\t\tTranscript: "+t.dump("|"));
                            }
                            //System.out.println(acc+" > 1");
                        }
                    }
                    for (int j = 1; j < parsedLine.length; j++){
                        if (gene==null && isGenes){
                            notFoundLog.info("\tGene: "+geneSymbol+" was not found or has been withdrawn!");
                            break;
                        }
                        if (transcript==null && !isGenes){
                            notFoundLog.info("\tTranscript: "+symbol+" was not found!");
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
                        if (isGenes)
                            v.setExpressedObjectRgdId(gene.getRgdId());
                        else
                            v.setExpressedObjectRgdId(transcript.getRgdId());
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
                        if (Utils.isStringEmpty(v.getExpressionMeasurementAccId()) || Utils.doublesAreEqual(value, 0.00,2))
                            continue;
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

    Map<String, Gene> getGenesFromGTF() throws Exception{
        Map<String, Gene> genesRgdMap = new HashMap<>();
        try (BufferedReader br = openFile(gtfFile)) {
            // chr gnomon type start stop skip skip skip geneIdLoc
            String lineData;
            while ((lineData = br.readLine()) != null) {
                if (lineData.startsWith("#"))
                    continue;
                String[] split = lineData.split("\t");
                String chr = split[0].replace("chr", "");
                int start = Integer.parseInt(split[3]);
                int stop = Integer.parseInt(split[4]);
                String part8 = split[8];
                // parse part8 to get gene id and find gene based on position
                String[] infoSplit = part8.split(";");
                String geneSymbol = infoSplit[0];
                geneSymbol = geneSymbol.replace("gene_id ", "");
                geneSymbol = geneSymbol.replace("\"", "");
                if (genesRgdMap.get(geneSymbol) != null)
                    continue;
                List<Gene> genes = dao.getActiveGenesBySymbol(geneSymbol.toLowerCase(), speciesType);
                if (!genes.isEmpty()) {
                    for (Gene g : genes){
                        if (Utils.stringsAreEqualIgnoreCase(g.getSymbol(), geneSymbol)) {
                            genesRgdMap.put(geneSymbol, g);
                            break;
                        }
                    }
                } else {
                    List<Gene> activeGenes = dao.getActiveGenesByLocation(chr, start, stop, mapKey);
                    if (activeGenes.size() == 1) {
                        genesRgdMap.put(geneSymbol, activeGenes.get(0));
                    } else if (!activeGenes.isEmpty()){
                        boolean found = false;
                        // loop through genes and find whichever one by alias if need be
                        for (Gene g : activeGenes) {
                            if (Utils.stringsAreEqualIgnoreCase(g.getSymbol(), geneSymbol)) {
                                genesRgdMap.put(geneSymbol, g);
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            List<Gene> geneList = dao.getGenesByAlias(geneSymbol, speciesType);
                            for (Gene gene : activeGenes) {
                                for (Gene alias : geneList) {
                                    if (gene.getRgdId() == alias.getRgdId()) {
                                        genesRgdMap.put(geneSymbol, alias);
                                        found = true;
                                        break;
                                    }
                                }
                                if (found)
                                    break;
                            }
                        }
                    }
                    else {// active genes is empty
                        List<Gene> geneList = dao.getGenesByAliasAndAliasType(geneSymbol, speciesType,"old_gene_symbol");
                        if (geneList.size()==1){
                            genesRgdMap.put(geneSymbol,geneList.get(0));
                        }
                        else if (!geneList.isEmpty()){
                            // ?
//                            genesRgdMap.put(geneSymbol,null);
                            logger.info("\t"+geneSymbol+" has multiple genes from Alias");
//                            System.out.println(geneList);
                        }
                    }
                } // end else
            } // end while
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return genesRgdMap;
    }

    public void setVersion(String version) {
        this.version=version;
    }

    public String getVersion() {
        return version;
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

    public void setStudy(String study) {
        this.study = study;
    }

    public String getStudy(){
        return study;
    }

    public void setGenesFile(String genesFile) {
        this.genesFile = genesFile;
    }

    public String getGenesFile() {
        return genesFile;
    }

    public void setIsoformFile(String isoformFile) {
        this.isoformFile = isoformFile;
    }

    public String getIsoformFile() {
        return isoformFile;
    }

    public void setGtfFile(String gtfFile) {
        this.gtfFile = gtfFile;
    }
}