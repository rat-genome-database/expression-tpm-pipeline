package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.datamodel.Gene;
import edu.mcw.rgd.datamodel.Transcript;
import edu.mcw.rgd.datamodel.pheno.ClinicalMeasurement;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecord;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecordValue;
import edu.mcw.rgd.datamodel.pheno.Sample;

import javax.sql.DataSource;
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
        return geneDAO.getActiveGenes(speciesType, symbol);
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
}
