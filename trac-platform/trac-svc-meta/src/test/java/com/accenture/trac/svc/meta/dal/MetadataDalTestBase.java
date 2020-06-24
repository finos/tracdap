package com.accenture.trac.svc.meta.dal;


public abstract class MetadataDalTestBase {

    protected IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }
}
