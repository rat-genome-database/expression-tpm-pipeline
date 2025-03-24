package edu.mcw.rgd.expressionTpm;

import edu.mcw.rgd.datamodel.GeoRecord;
import edu.mcw.rgd.datamodel.Reference;
import edu.mcw.rgd.datamodel.XdbId;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.ontologyx.TermSynonym;
import edu.mcw.rgd.datamodel.pheno.Condition;
import edu.mcw.rgd.datamodel.pheno.GeneExpressionRecord;
import edu.mcw.rgd.datamodel.pheno.Sample;
import edu.mcw.rgd.datamodel.pheno.Study;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MetaDataLoader {
    private String version;
    private String studiesList;
    private String species;

    private DAO dao = new DAO();
    protected Logger logger = LogManager.getLogger("downloadStatus");
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void main() throws Exception{
        // read list file and loop through to generate files
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        List<String> studies = new ArrayList<>();
        try (BufferedReader br = dao.openFile(studiesList)){
            String lineData;
            while ((lineData = br.readLine()) != null){
                studies.add(lineData);
            }
        }
        catch (Exception e){
            Utils.printStackTrace(e, logger);
        }

        for (String gse : studies){
            logger.info("Generating Meta Data for GEO study: "+gse);
            generateFiles(gse);
            logger.info("================================================");
        }

        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");

    }

    void generateFiles(String gse) throws Exception{
        String eSearchUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=biosample&term="; // use gsm
        String eLinkUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=biosample&db=sra&id="; // use gsm ID from search
        String eSummaryUrl = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=sra&id="; // use SRA ID from link


        // get the title from the rna_seq table
        List<GeoRecord> rnaSeqRec = dao.getGeoRecords(gse, species);
        HashMap<String, GeoRecord> geoRecMap = new HashMap<>();
        if (rnaSeqRec == null || rnaSeqRec.isEmpty()) {
            logger.info("\tNo RNA_SEQ data!");
            return;
        }
        else {
            geoRecMap = assignRecords(rnaSeqRec);
        }
        String title = rnaSeqRec.get(0).getStudyTitle();

        Study study = dao.getStudyByGeoIdWithReferences(gse);
        String pmids = "";
        if (study != null) {
            for (int i = 0; i < study.getRefRgdIds().size(); i++) {
                Integer rgdId = study.getRefRgdIds().get(i);
                Reference r = dao.getReferenceByRgdId(rgdId);
                List<XdbId> xdbs = dao.getXdbIdsByRgdId(2, r.getRgdId());
                XdbId xdb = xdbs.get(0);
//                xdbIdDAO.getAllXdbIdsByRgdId()
                if (i == study.getRefRgdIds().size() - 1) {
                    pmids += xdb.getAccId();
                } else {
                    pmids += xdb.getAccId() + ";";
                }

            }
        }
        if (Utils.isStringEmpty(pmids))
            pmids=null;

        List<Sample> samples = dao.getGeoRecordSamplesByStatus(gse,species,"loaded");
        HashMap<String, String> sampleConditionsMap = new HashMap<>();
        HashMap<String, Term> tissueMap = new HashMap<>();
        HashMap<String, Term> strainMap = new HashMap<>();
        HashMap<String, String> strainLinkMap = new HashMap<>();
        HashMap<String, List<String>> sampleSRR = new HashMap<>();
        for (Sample s : samples){
            // get conditions and apply to map
            GeoRecord geoRec = geoRecMap.get(s.getGeoSampleAcc());
            if (Utils.isStringEmpty(pmids))
                pmids = geoRec != null ? geoRec.getPubmedId() : null;
            if (!Utils.isStringEmpty(s.getTissueAccId()) && tissueMap.get(s.getTissueAccId())==null) {
                Term tissue = dao.getTermByAccId(s.getTissueAccId().trim());
                tissueMap.put(s.getTissueAccId(), tissue);
            }
            if (!Utils.isStringEmpty(s.getStrainAccId()) && strainMap.get(s.getStrainAccId())==null){
                Term strain = dao.getTermByAccId(s.getStrainAccId().trim());
                strainMap.put(s.getStrainAccId(), strain);
                List<TermSynonym> syns = dao.getTermSynonyms(strain.getAccId());

                for (TermSynonym syn : syns){
                    if (syn.getName().startsWith("RGD")){
                        String strainSyn = syn.getName().replace("RGD ID: ","");
                        strainLinkMap.put(strain.getAccId(),strainSyn);
                        break;
                    }
                }
                strainLinkMap.putIfAbsent(s.getGeoSampleAcc(), "");
            }
            try {
                Document doc = Jsoup.connect(eSearchUrl + s.getGeoSampleAcc()).get(); // getting ncbi ID for GSM
                Elements idlist = doc.select("IdList");
                Element gsmList = idlist.get(0);
                String gsmId = gsmList.text();
//                String body = doc.data();
                Thread.sleep(3000); // wait a second to not hammer ncbi with requests
                Document sra = Jsoup.connect(eLinkUrl + gsmId).get(); // getting SRA ID from GSM Id
                Elements sraLink = sra.select("Link");
                Element linkId = sraLink.get(0);
                String sraId = linkId.text();
                Thread.sleep(3000);
                Document summary = Jsoup.connect(eSummaryUrl + sraId).get();
                Elements items = summary.select("Item");
                Element runs = null;
                for (int i = 0; i < items.size(); i++) {
                    Element e = items.get(i);
                    String name = e.attr("Name");
                    if (Utils.stringsAreEqualIgnoreCase(name, "Runs")) {
                        runs = e;
                        break;
                    }
                }
                String[] accSplit = runs.text().split("<Run acc=\"");

                List<String> srrIds = new ArrayList<>();
                if (accSplit.length > 2) {
                    for (int x = 1; x < accSplit.length; x++) {
                        if (!accSplit[x].startsWith("SRR"))
                            continue;
                        int index = accSplit[x].indexOf("\"");
                        String srrId = accSplit[x].substring(0, index);
                        srrIds.add(srrId);
                    }
                } else {
                    int index = accSplit[1].indexOf("\"");
                    String srrId = accSplit[1].substring(0, index);
                    srrIds.add(srrId);
                }
//                System.out.println("SRR IDS for "+s.getGeoSampleAcc()+": "+srrIds.size());
                sampleSRR.put(s.getGeoSampleAcc(), srrIds);
                Thread.sleep(3000);
            }
            catch (Exception e){
                // unable to find SRR or error with NCBI
//                e.printStackTrace();
                sampleSRR.put(s.getGeoSampleAcc(),new ArrayList<>());
            }
            GeneExpressionRecord r = dao.getGeneExpressionRecordBySampleId(s.getId());
            List<Condition> conditions = dao.getConditions(r.getId());
            String condNames = "";
            for (int i = 0; i < conditions.size(); i++){
                Condition c = conditions.get(i);
                Term t = dao.getTermByAccId(c.getOntologyId());
                if (i == conditions.size()-1) {
                    condNames += t.getTerm();
                }
                else {
                    condNames += t.getTerm()+";";
                }
            }
            if (Utils.isStringEmpty(condNames))
                sampleConditionsMap.put(s.getGeoSampleAcc(),null);
            else
                sampleConditionsMap.put(s.getGeoSampleAcc(),condNames);
        }

//            request.setAttribute("gse", gse);
//            request.setAttribute("samples",samples);
//            request.setAttribute("title",title);
//            request.setAttribute("conditionMap", sampleConditionsMap);
//            request.setAttribute("tissueMap", tissueMap);
//            request.setAttribute("strainMap", strainMap);
//            request.setAttribute("sampleSrrMap", sampleSRR);
//            request.setAttribute("strainSynMap", strainLinkMap);




        // create buffer writer and generate files like in web app
        try(BufferedWriter bw = new BufferedWriter(new FileWriter("data/metaData/"+gse+"_AccList.txt")) ){
            String geoPath = "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc="+gse;
            String strainPath = "https://rgd.mcw.edu/rgdweb/report/strain/main.html?id=";
            bw.write("Run\tgeo_accession\tTissue\tStrain\tSex\tPMID\tGEOpath\tTitle\tSample_characteristics\tStrainInfo");
            bw.write("\n");
            for(Sample s: samples){
//        System.out.println(s.getGeoSampleAcc());
                if (Utils.isStringEmpty(pmids))
                    pmids=null;
                Term tis = tissueMap.get(s.getTissueAccId());
                String term = null;
                if (tis != null){
                    term = tis.getTerm();
                    term = term.replace(" ","_");
                }
                Term str = strainMap.get(s.getStrainAccId());
                String strain = null;
                if (str != null){
                    strain = str.getTerm().replaceAll("[*!<>?\"|]","");
                    strain = strain.replaceAll("[:\\\\/() .]","_");
                    strain = strain.replace("__","_");
                    strain = strain.replace("-_+","MUT");
                    strain = strain.replace("+_-","MUT");
                    strain = strain.replace("-_-","MUT");
                    strain = strain.replace("+_+","WT");
                    if (strain.endsWith("_"))
                        strain=strain.substring(0,strain.length()-1);
                }
                String conds = sampleConditionsMap.get(s.getGeoSampleAcc());
                List<String> srrIds = sampleSRR.get(s.getGeoSampleAcc());
                String strainId = strainLinkMap.get(s.getStrainAccId());
                if (srrIds.size()>1){
                    for (String srrId : srrIds){
                        bw.write(Utils.NVL(srrId,"NA"));
                        bw.write("\t");
                        bw.write(s.getGeoSampleAcc());
                        bw.write("\t");
                        bw.write(Utils.NVL(term,"NA"));
                        bw.write("\t");
                        bw.write(Utils.NVL(strain,"NA"));
                        bw.write("\t");
                        if (Utils.stringsAreEqualIgnoreCase(s.getSex(),"male")){
                            bw.write("M");
                        } else if(Utils.stringsAreEqualIgnoreCase(s.getSex(),"female")){
                            bw.write("F");
                        } else if(Utils.stringsAreEqualIgnoreCase(s.getSex(),"both")){
                            bw.write("both");
                        } else {
                            bw.write("not_specified");
                        }
                        bw.write("\t");
                        bw.write(Utils.NVL(pmids,"NA"));
                        bw.write("\t");
                        bw.write(geoPath);
                        bw.write("\t");
                        bw.write(title);
                        bw.write("\t");
                        bw.write(Utils.NVL(conds,"NA"));
                        bw.write("\t");
                        if (!Utils.isStringEmpty(strainId))
                            bw.write(strainPath+strainId);
                        else
                            bw.write("NA");
                        bw.write("\n");
                    }
                }
                else if (srrIds.size()==1){
                    bw.write(srrIds.get(0));
                    bw.write("\t");
                    bw.write(s.getGeoSampleAcc());
                    bw.write("\t");
                    bw.write(Utils.NVL(term,"NA"));
                    bw.write("\t");
                    bw.write(Utils.NVL(strain,"NA"));
                    bw.write("\t");
                    if (Utils.stringsAreEqualIgnoreCase(s.getSex(),"male")){
                        bw.write("M");
                    } else if(Utils.stringsAreEqualIgnoreCase(s.getSex(),"female")){
                        bw.write("F");
                    } else if(Utils.stringsAreEqualIgnoreCase(s.getSex(),"both")){
                        bw.write("both");
                    } else {
                        bw.write("not_specified");
                    }
                    bw.write("\t");
                    bw.write(Utils.NVL(pmids,"NA"));
                    bw.write("\t");
                    bw.write(geoPath);
                    bw.write("\t");
                    bw.write(title);
                    bw.write("\t");
                    bw.write(Utils.NVL(conds,"NA"));
                    bw.write("\t");
                    if (!Utils.isStringEmpty(strainId))
                        bw.write(strainPath+strainId);
                    else
                        bw.write("NA");
                    bw.write("\n");
                }
                else{
                    bw.write("NA");
                    bw.write("\t");
                    bw.write(s.getGeoSampleAcc());
                    bw.write("\t");
                    bw.write(Utils.NVL(term,"NA"));
                    bw.write("\t");
                    bw.write(Utils.NVL(strain,"NA"));
                    bw.write("\t");
                    if (Utils.stringsAreEqualIgnoreCase(s.getSex(),"male")){
                        bw.write("M");
                    } else if(Utils.stringsAreEqualIgnoreCase(s.getSex(),"female")){
                        bw.write("F");
                    } else if(Utils.stringsAreEqualIgnoreCase(s.getSex(),"both")){
                        bw.write("both");
                    } else {
                        bw.write("not_specified");
                    }
                    bw.write("\t");
                    bw.write(Utils.NVL(pmids,"NA"));
                    bw.write("\t");
                    bw.write(geoPath);
                    bw.write("\t");
                    bw.write(title);
                    bw.write("\t");
                    bw.write(Utils.NVL(conds,"NA"));
                    bw.write("\t");
                    if (!Utils.isStringEmpty(strainId))
                        bw.write(strainPath+strainId);
                    else
                        bw.write("NA");
                    bw.write("\n");
                }

            }
        }
        catch (Exception e){
            Utils.printStackTrace(e, logger);
        }



    }

    HashMap<String, GeoRecord> assignRecords(List<GeoRecord> geoRecords) throws Exception{
        HashMap<String, GeoRecord> recMap = new HashMap<>();
        for (GeoRecord r : geoRecords){
            recMap.put(r.getSampleAccessionId(), r);
        }
        return recMap;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setStudiesList(String studiesList) {
        this.studiesList = studiesList;
    }

    public String getStudiesList() {
        return studiesList;
    }

    public void setSpecies(String species) {
        this.species = species;
    }

    public String getSpecies() {
        return species;
    }
}
