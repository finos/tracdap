import {Alert} from "../../../components/Alert";
import {General} from "../../../../config/config_general";
import React from "react";
import {SchemaEditor} from "../../../components/SchemaEditor/SchemaEditor";
import {updateSchema} from "../store/uploadADatasetStore";
import {useAppSelector} from "../../../../types/types_hooks";

/**
 * A component that allows the user to edit the suggested schema for a dataset loaded from a local file.
 */

const SuggestedSchemaTab = () => {

    // Get what we need from the store
    const {guessedVariableTypes} = useAppSelector(state => state["uploadADatasetStore"].import)
    const {schema} = useAppSelector(state => state["uploadADatasetStore"].file)
    const {isSuggestedSchemaTheSame} = useAppSelector(state => state["uploadADatasetStore"].alreadyInTrac)

    return (
        <React.Fragment>

            {guessedVariableTypes &&
                <SchemaEditor canEditFieldName={false}
                              dispatchedUpdateSchema={updateSchema}
                              schema={schema}
                              guessedVariableTypes={guessedVariableTypes}
                />
            }

            {!General.loading.allowCopies.data && isSuggestedSchemaTheSame &&
                <Alert className={"mt-4 mb-3"} variant={"warning"}>
                    <div>
                        The field names and types are identical to the version stored in the dataset
                        already loaded into TRAC. If you need to edit the field names or types then that
                        can be done by changing them above and loading the new version.
                    </div>
                </Alert>
            }

        </React.Fragment>
    )
};

export default SuggestedSchemaTab;