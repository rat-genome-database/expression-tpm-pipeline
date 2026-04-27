package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.spring.GeneQuery;
import edu.mcw.rgd.dao.spring.PhenoSampleQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.datamodel.pheno.*;
import edu.mcw.rgd.datamodel.pheno.Sample;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.SqlParameter;

import java.io.*;
import java.sql.Types;
import java.util.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    private OntologyXDAO xdao = new OntologyXDAO();
    private GeneDAO geneDAO = new GeneDAO();
    private GeneExpressionDAO gedao = new GeneExpressionDAO();
    private PhenominerDAO pdao = new PhenominerDAO();
    private TranscriptDAO tdao = new TranscriptDAO();
    private MapDAO mdao = new MapDAO();
    private ReferenceDAO rdao = new ReferenceDAO();
    private XdbIdDAO xdbIdDAO = new XdbIdDAO();

    public String getConnection(){
        return geneDAO.getConnectionInfo();
    }

    public Sample getSampleByGeoSampleName(String geoSample) throws Exception{
        return pdao.getSampleByGeoId(geoSample);
    }

    public List<Sample> getGeoRecordSamplesByStatus(String geoId, String species, String status) throws Exception{
        return pdao.getGeoRecordSamplesByStatus(geoId,species,status);
    }

    public GeneExpressionRecord getGeneExpressionRecordBySample(int sampleId) throws Exception{
        return gedao.getGeneExpressionRecordBySampleId(sampleId);
    }

    public List<Gene> getActiveGenesBySymbol(String symbol, int speciesType) throws Exception{
        return geneDAO.getAllActiveGenesBySymbol(symbol, speciesType);
    }

    public Gene getGeneBySymbol(String symbol, int speciesTypeKey) throws Exception{
        return geneDAO.getGenesBySymbol(symbol,speciesTypeKey);
    }

    public Gene getGeneByRgdId(int rgdId) throws Exception{
        return geneDAO.getGene(rgdId);
    }

    public List<Gene> getGenesByAlias(String alias, int species) throws Exception{
        return geneDAO.getActiveGenesByAlias(alias,species);
    }

    public List<Gene> getGenesByAliasAndAliasType(String alias, int species, String aliasType) throws Exception{
        String sql = "SELECT g.*,r.SPECIES_TYPE_KEY from GENES g, RGD_IDS r, ALIASES a where a.ALIAS_VALUE_LC=LOWER(?) and g.RGD_ID=r.RGD_ID and r.SPECIES_TYPE_KEY=? and a.RGD_ID=g.RGD_ID and r.OBJECT_STATUS='ACTIVE' and a.alias_type_name_lc=?";
        GeneQuery q = new GeneQuery(geneDAO.getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        return q.execute(alias, species,aliasType);
    }

    public Sample getSampleByHRDPSampleName(String sampleName) throws Exception {
        String sql = "select * from SAMPLE where to_char(SAMPLE_NOTES)=?";
        PhenoSampleQuery q = new PhenoSampleQuery(pdao.getDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        List<Sample> samples =  q.execute(sampleName);
        if (samples.isEmpty())
            return null;
        return samples.get(0);
    }

    public List<Gene> getGenesByEnsemblSymbol(int speciesTypeKey, String symbol) throws Exception{
        return geneDAO.getActiveGenesByEnsemblSymbol(speciesTypeKey,symbol);
    }

    public ClinicalMeasurement getClinicalMeasurement(int cmoId) throws Exception{
        return pdao.getClinicalMeasurement(cmoId);
    }

    public List<GeneExpressionRecordValue> getGeneExpressionRecordValuesByRecordIdAndExpressedObjId(int recId, int objId, String unit) throws Exception{
        return gedao.getGeneExpressionRecordValuesByRecordIdAndExpressedObjId(recId,objId,unit);
    }

    public int insertExpressionRecordValues(Collection<GeneExpressionRecordValue> incoming) throws Exception{
        return gedao.insertGeneExpressionRecordValueBatch(incoming);
    }

    public List<Transcript> getTranscriptsByAccId(String acc) throws Exception{
        return tdao.getTranscriptsByAccId(acc);
    }

    public List<MapData> getMapDataByRgdIdAndMapKey(int rgdId, int mapKey) throws Exception {
        return mdao.getMapData(rgdId, mapKey);
    }

    public List<Gene> getActiveGenesByLocation(String chr, int start, int stop, int mapKey) throws Exception{
        return geneDAO.getActiveGenesNSource(chr,start,stop,mapKey,"NCBI");
    }

    public Term getTermByAccId(String accId) throws Exception{
        return xdao.getTermByAccId(accId);
    }

    public List<TermSynonym> getTermSynonyms(String accId) throws Exception{
        return xdao.getTermSynonyms(accId);
    }

    public GeneExpressionRecord getGeneExpressionRecordBySampleId(int sampleId) throws Exception {
        return gedao.getGeneExpressionRecordBySampleId(sampleId);
    }

    public GeneExpressionRecord getGeneExpressionRecord(int id) throws Exception {
        return gedao.getGeneExpressionRecordById(id);
    }

    public List<Condition> getConditions(int recordId) throws Exception{
        return gedao.getConditions(recordId);
    }

    public List<GeoRecord> getGeoRecords(String geoId, String species) throws Exception {
        return pdao.getGeoRecords(geoId, species);
    }

    public Study getStudyByGeoIdWithReferences(String gse) throws Exception {
        return gedao.getStudyByGeoIdWithReferences(gse);
    }

    public Reference getReferenceByRgdId(int rgdId) throws Exception {
        return rdao.getReferenceByRgdId(rgdId);
    }

    public List<XdbId> getXdbIdsByRgdId(int xdbKey, int rgdId) throws Exception {
        return xdbIdDAO.getXdbIdsByRgdId(xdbKey, rgdId);
    }

    public void updateComputedSex(List<Sample> samples) throws Exception {
        pdao.updateSampleComputedSexBatch(samples);
    }

    public String getStrainOntIdForRgdId(int rgdId) throws Exception {
        return xdao.getStrainOntIdForRgdId(rgdId);
    }

    public int insertClinicalMeasurement(ClinicalMeasurement cmo) throws Exception {
        return gedao.insertClinicalMeasurement(cmo);
    }

    public int insertExperiment(Experiment e) throws Exception {
        return pdao.insertExperiment(e);
    }

    public int insertSample(Sample s) throws Exception {
        return pdao.insertSample(s);
    }

    public int insertGeneExpressionRecord(GeneExpressionRecord gre) throws Exception {
        return gedao.insertGeneExpressionRecord(gre);
    }

    public int insertCondition(Condition c) throws Exception {
        return pdao.insertCondition(c);
    }

    public Study getStudy(int studyId) throws Exception {
        return pdao.getStudy(studyId);
    }

    public List<Experiment> getExperiments(int studyId) throws Exception {
        return pdao.getExperiments(studyId);
    }

    public BufferedReader openFile(String fileName) throws IOException {

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

    public void listFilesInFolder(File folder, ArrayList<File> vcfFiles) throws Exception {
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (file.isDirectory()) {
                listFilesInFolder(file,vcfFiles);
            } else {
//                    System.out.println(file.getName());
                vcfFiles.add(file);
            }
        }
    }
    public Map<String, Gene> getGenesFromGTF(String gtfFile, int speciesType, int mapKey, Logger logger, Logger notFoundLog) throws Exception{
        java.util.Map<String, Gene> genesRgdMap = new HashMap<>();
        Map<String, Integer> notFoundGenes = new HashMap<>();
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
                if (genesRgdMap.get(geneSymbol) != null || notFoundGenes.get(geneSymbol) != null)
                    continue;

                List<Gene> genes = getActiveGenesBySymbol(geneSymbol.toLowerCase(), speciesType);
                if (!genes.isEmpty()) {
                    for (Gene g : genes){
                        if (Utils.stringsAreEqualIgnoreCase(g.getSymbol(), geneSymbol)) {
                            genesRgdMap.put(geneSymbol, g);
                            break;
                        }
                    }
                } else if (geneSymbol.startsWith("RGD"))
                {
                    int rgdId = Integer.parseInt(geneSymbol.replace("RGD", ""));
                    Gene gene = getGeneByRgdId(rgdId);
                    genesRgdMap.put(geneSymbol, gene);
                }
                else {
                    List<Gene> activeGenes = getActiveGenesByLocation(chr, start, stop, mapKey);
                    if (activeGenes.size() == 1) {
                        Gene foundGene = activeGenes.get(0);
                        if (Utils.stringsAreEqualIgnoreCase(foundGene.getSymbol(), geneSymbol)){
                            genesRgdMap.put(geneSymbol, activeGenes.get(0));
                        }
                        else {
//                            notFoundGenes.put(geneSymbol,1);
                            notFoundLog.info("Gene " + geneSymbol + " overlaps active Gene " + foundGene.getSymbol());
                        }

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
                            List<Gene> geneList = getGenesByAlias(geneSymbol, speciesType);
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
                        List<Gene> geneList = getGenesByAliasAndAliasType(geneSymbol, speciesType,"old_gene_symbol");
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
                if (genesRgdMap.get(geneSymbol)==null){
                    notFoundGenes.put(geneSymbol,1);
                    notFoundLog.info("\tGene: " + geneSymbol + " was not found or has been withdrawn!");
                }
            } // end while
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return genesRgdMap;
    }

    public boolean checkValueExists(GeneExpressionRecordValue incomingValue) throws Exception{
        List<GeneExpressionRecordValue> existingValues = getGeneExpressionRecordValuesByRecordIdAndExpressedObjId(incomingValue.getGeneExpressionRecordId(),incomingValue.getExpressedObjectRgdId(),"TPM");
        if (existingValues.isEmpty())
            return false;
        for (GeneExpressionRecordValue v : existingValues){
            if (Double.compare(v.getExpressionValue(),incomingValue.getExpressionValue()) == 0 &&
                    Utils.stringsAreEqual(v.getExpressionMeasurementAccId(),incomingValue.getExpressionMeasurementAccId()) )
                return true;
        }
        return false;
    }
}
