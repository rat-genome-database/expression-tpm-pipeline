package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.spring.GeneQuery;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.MapData;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.datamodel.pheno.ClinicalMeasurement;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecord;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecordValue;
import edu.mcw.rgd.datamodel.pheno.Sample;
import org.springframework.jdbc.core.SqlParameter;

import javax.sql.DataSource;
import java.sql.Types;
import java.util.Collection;
import java.util.List;

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

    public String getConnection(){
        return geneDAO.getConnectionInfo();
    }

    public Sample getSampleByGeoSampleName(String geoSample) throws Exception{
        return pdao.getSampleByGeoId(geoSample);
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
}
