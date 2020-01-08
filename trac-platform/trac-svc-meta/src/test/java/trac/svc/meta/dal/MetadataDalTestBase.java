package trac.svc.meta.dal;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;


public abstract class MetadataDalTestBase {

    protected IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }
}
