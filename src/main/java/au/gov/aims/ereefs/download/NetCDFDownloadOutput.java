/*
 * Copyright (c) Australian Institute of Marine Science, 2021.
 * @author Gael Lafond <g.lafond@aims.gov.au>
 */
package au.gov.aims.ereefs.download;

import au.gov.aims.ereefs.bean.metadata.netcdf.NetCDFMetadataBean;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class NetCDFDownloadOutput {
    private List<NetCDFMetadataBean> success;
    private List<String> errors;
    private List<String> warnings;

    public NetCDFDownloadOutput() {
        this.success = new ArrayList<NetCDFMetadataBean>();
        this.errors = new ArrayList<String>();
        this.warnings = new ArrayList<String>();
    }

    public void addSuccess(NetCDFMetadataBean metadata) {
        this.success.add(metadata);
    }

    public void addError(String errorMsg) {
        this.errors.add(errorMsg);
    }

    public void addWarning(String warningMsg) {
        this.warnings.add(warningMsg);
    }

    public boolean isEmpty() {
        return this.success.isEmpty() && this.errors.isEmpty() && this.warnings.isEmpty();
    }

    public List<NetCDFMetadataBean> getSuccess() {
        return this.success;
    }

    public List<String> getErrors() {
        return this.errors;
    }

    public List<String> getWarnings() {
        return this.warnings;
    }

    public JSONObject toJSON() {
        JSONObject json = new JSONObject();

        if (this.success != null && !this.success.isEmpty()) {
            JSONArray successArray = new JSONArray();
            for (NetCDFMetadataBean successBean : this.success) {
                successArray.put(successBean.toJSON());
            }
            json.put("success", successArray);
        }

        if (this.warnings != null && !this.warnings.isEmpty()) {
            json.put("warnings", new JSONArray(this.warnings));
        }

        if (this.errors != null && !this.errors.isEmpty()) {
            json.put("errors", new JSONArray(this.errors));
        }

        return json;
    }

    @Override
    public String toString() {
        return this.toJSON().toString(4);
    }
}
