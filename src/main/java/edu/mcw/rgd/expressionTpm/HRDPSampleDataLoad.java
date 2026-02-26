package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.Strain;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.datamodel.pheno.*;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class HRDPSampleDataLoad {
    private String version;
    private String hrdpSampleFile;
    private String gtfFile;
    private int speciesType;
    private int mapKey;
    private Map<String,String> tissueMap;
    private Map<String,String> cmoMap;
    private Map<String,String> vtMap;
    private Map<String, String> conditionMap;
    private int studyId;
    private String matrixFile;
    private DAO dao = new DAO();
    protected Logger logger = LogManager.getLogger("hrdpStatus");
    protected Logger notFoundLog = LogManager.getLogger("notFoundStatus");
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



    public void run() throws Exception{
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");
        Map<String, Sample> sampleMap = new HashMap<>();
        Map<Integer, GeneExpressionRecord> greMap = new HashMap<>();
        try (BufferedReader br = dao.openFile(hrdpSampleFile)) {
            logger.info("\tStarting load of HRDP samples");
            Study study = new Study();
            study = dao.getStudy(studyId);
            List<Experiment> experiments = dao.getExperiments(studyId);
            Map<String, Experiment> experimentMap = new HashMap<>();
            String lineData;
            while ((lineData = br.readLine()) != null) {
                String[] cols = lineData.split("\t");
                // col 0 run/srr id... ignore
                if (lineData.startsWith("Run"))
                    continue;
                Sample s = dao.getSampleByHRDPSampleName(cols[1]);

                GeneExpressionRecord gre = new GeneExpressionRecord();
                ClinicalMeasurement cmo = new ClinicalMeasurement();
                int cmoId = 0;
                // get sample with notes using sample name

                gre.setCurationStatus(35);
                gre.setSpeciesTypeKey(speciesType);
                String vtAccId = getVtMap().get(cols[2]);
                if (experimentMap.get(vtAccId)==null){
                    // check db first, if none create it
                    Experiment e = null;
                    for (Experiment exp : experiments){
                        if (Utils.stringsAreEqual(exp.getTraitOntId(),vtAccId)) {
                            e = exp;
                            break;
                        }
                    }
                    if (e==null){// create new experiment
                        e = new Experiment();
                        e.setStudyId(studyId);
                        e.setName(vtAccId);
                        e.setCreatedBy("Expression-tpm-Pipeline");
                        e.setLastModifiedBy("Expression-tpm-Pipeline");
                        e.setTraitOntId(vtAccId);
                        int expId = dao.insertExperiment(e);
                        gre.setExperimentId(expId);
                    }
                    experimentMap.put(vtAccId,e);
                }
                else
                    gre.setExperimentId(experimentMap.get(vtAccId).getId());
                // col 3 strain... ignore, get from col 9
                // col 4 sex

                // col 5 pmid use to get ref rgd id and
                // col 6 geopath... ignore
                // col 7 title, ignore
                // col 8 sample chars... has xco
                Condition c = new Condition();
                c.setOrdinality(1);
                c.setOntologyId(conditionMap.get(cols[8]));
                // col 9 strain link
                String[] strainArr = cols[9].split("=");
                String strainRgdIdStr = strainArr[strainArr.length-1];
                int strainRgdId = Integer.parseInt(strainRgdIdStr);
                String strainRSId = dao.getStrainOntIdForRgdId(strainRgdId);
                // enter sample first, then gre, then condition
                int sampleId;
                if (s==null) {
                    s = new Sample();
                    s.setNotes(cols[1]); // col 1 hrdp sample name, put in notes
                    s.setTissueAccId(getTissueMap().get(cols[2])); // col 2 tissue, cmo in gre, vt in experiment
                    s.setStrainAccId(strainRSId);
                    if (Utils.stringsAreEqualIgnoreCase(cols[4],"M"))
                        s.setSex("male");
                    else
                        s.setSex("female");
                    sampleId = dao.insertSample(s);
                    sampleMap.put(s.getNotes(),s);
                }
                else {
                    sampleId = s.getId();
                    sampleMap.put(s.getNotes(),s);
                }
                int greId;
                GeneExpressionRecord greDb = dao.getGeneExpressionRecordBySampleId(sampleId);
                if (greDb == null) {
                    gre.setSampleId(sampleId);
                    cmo = new ClinicalMeasurement();
                    cmo.setAccId(getCmoMap().get(cols[2]));
                    cmoId = dao.insertClinicalMeasurement(cmo);
                    gre.setClinicalMeasurementId(cmoId);
                    gre.setLastModifiedBy("Expression-tpm-pipeline");
                    greId = dao.insertGeneExpressionRecord(gre);
                    c.setGeneExpressionRecordId(greId);
                    dao.insertCondition(c);
                    greMap.put(sampleId, gre);
                }
                else {
                    greId = greDb.getId();
                    greMap.put(sampleId, gre);
                    List<Condition> greConds = dao.getConditions(greId);
                    for (Condition con : greConds){
                        if (!Utils.stringsAreEqual(c.getOntologyId(),con.getOntologyId()) || !Objects.equals(c.getOrdinality(), con.getOrdinality()))
                        {
                            c.setGeneExpressionRecordId(greId);
                            dao.insertCondition(c);
                        }
                    }
                }

            }
            logger.info("\tEnd of HRDP sample load");
        } // end meta data file insert
        catch (Exception e){
            logger.info(e);
        }
        try {
            Map<String, Gene> geneMap = new HashMap<>();
            // start the tpm insert process
            geneMap = dao.getGenesFromGTF(gtfFile, speciesType, mapKey, logger, notFoundLog);
            File file = new File(matrixFile);
            Map<Integer, GeneExpressionRecord> recordMap = new HashMap<>();
            Map<Integer, String> cmoMap = new HashMap<>();
            List<GeneExpressionRecordValue> values = new ArrayList<>();

            try (BufferedReader br = dao.openFile(file.getAbsolutePath())) {
                String lineData;
                int i = 0;
                while ((lineData = br.readLine()) != null) {
                    String[] parsedLine = lineData.split("\t");
                    if (i == 0) {// row 0 is GEO sample names
                        int j = 0;
                        for (String col : parsedLine) {
                            if (j > 0) {
                                String sampleName = "";
                                String[] parseCol = col.split("\\.");
                                sampleName = parseCol[0].substring(1);
//                            System.out.println(sampleName);
                                Sample s = sampleMap.get(sampleName);
                                if (s == null) {
                                    logger.info("\t" + sampleName + " does not exist!");
                                    recordMap.put(j, null);
                                    cmoMap.put(j, null);
                                } else {
                                    GeneExpressionRecord r = greMap.get(s.getId());
                                    GeneExpressionRecord greDb = dao.getGeneExpressionRecordBySample(s.getId());
                                    if (greDb != null)
                                    {
                                        r = greDb;
                                        greMap.put(s.getId(), r);
                                    }
                                    recordMap.put(j, r);
                                    cmoMap.put(j, s.getCellTypeAccId());
                                }
                            }
                            j++;
                        }
                    } // end first row
                    else {
                        String symbol = parsedLine[0];
                        String geneSymbol = "";
                        Gene gene = null;
                        geneSymbol = symbol.replace("\"", "");
                        gene = geneMap.get(geneSymbol);

                        for (int j = 1; j < parsedLine.length; j++) {
                            if (gene == null) {
                                //notFoundLog.info("\tGene: " + geneSymbol + " was not found or has been withdrawn!");
                                break;
                            }
                            if (recordMap.get(j) == null) { // there is no record
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
                            if (Utils.doublesAreEqual(value, 0.00, 2))
                                continue;
                            if (!dao.checkValueExists(v))
                                values.add(v);
                        }
                    }

                    i++;
                }
                if (!values.isEmpty()) {
                    logger.info("\t\tExpression Values being entered: " + values.size());
                    dao.insertExpressionRecordValues(values);
                }
            } catch (Exception e) {
                Utils.printStackTrace(e, logger);
            }
        }
        catch (Exception e){
            logger.info(e);
        }
        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setHrdpSampleFile(String hrdpSampleFile) {
        this.hrdpSampleFile = hrdpSampleFile;
    }

    public String getHrdpSampleFile() {
        return hrdpSampleFile;
    }

    public void setGtfFile(String gtfFile) {
        this.gtfFile = gtfFile;
    }

    public String getGtfFile() {
        return gtfFile;
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

    public void setTissueMap(Map<String,String> tissueMap) {
        this.tissueMap = tissueMap;
    }

    public Map<String,String> getTissueMap() {
        return tissueMap;
    }
    public void setCmoMap(Map<String,String> cmoMap) {

        this.cmoMap = cmoMap;
    }

    public Map<String,String> getCmoMap() {
        return cmoMap;
    }

    public void setVtMap(Map<String,String> vtMap) {
        this.vtMap = vtMap;
    }

    public Map<String,String> getVtMap() {
        return vtMap;
    }

    public void setStudyId(int studyId) {
        this.studyId = studyId;
    }

    public int getStudyId() {
        return studyId;
    }

    public void setConditionMap(Map<String,String> conditionMap) {
        this.conditionMap = conditionMap;
    }

    public Map<String,String> getConditionMap() {
        return conditionMap;
    }

    public void setMatrixFile(String matrixFile) {
        this.matrixFile = matrixFile;
    }

    public String getMatrixFile() {
        return matrixFile;
    }
}
