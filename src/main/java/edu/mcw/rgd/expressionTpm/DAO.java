package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.spring.GeneQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.datamodel.pheno.*;
import edu.mcw.rgd.datamodel.pheno.Sample;
import org.springframework.jdbc.core.SqlParameter;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
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
}
