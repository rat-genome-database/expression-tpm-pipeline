package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.datamodel.Gene;
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

    public String getConnection(){
        return geneDAO.getConnectionInfo();
    }

    public Sample getSampleByGeoSampleName(String geoSample) throws Exception{
        return pdao.getSampleByGeoId(geoSample);
    }

    public GeneExpressionRecord getGeneExpressionRecordBySample(int sampleId) throws Exception{
        return gedao.getGeneExpressionRecordBySampleId(sampleId);
    }

    public Gene getGeneBySymbol(String symbol, int speciesTypeKey) throws Exception{
        return geneDAO.getGenesBySymbol(symbol,speciesTypeKey);
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
}
