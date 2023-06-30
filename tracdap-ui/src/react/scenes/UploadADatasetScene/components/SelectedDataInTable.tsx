import {HeaderTitle} from "../../../components/HeaderTitle";
import {Loading} from "../../../components/Loading";
import React from "react";
import {Table} from "../../../components/Table/Table";
import {useAppSelector} from "../../../../types/types_hooks";
import {ImportedFileSchema} from "../../../../types/types_general";

/**
 * A component that shows the data from an imported csv or Excel file in a table.
 */

const SelectedDataInTable = () => {

    // Get what we need from the store
    const {data, schema: guessedSchema} = useAppSelector(state => state["uploadADatasetStore"].file)
    const {selectedOption, selectedTab} = useAppSelector(state => state["uploadADatasetStore"].existingSchemas)
    const {status} = useAppSelector(state => state["uploadADatasetStore"].import)
    const {fileInfo} = useAppSelector(state => state["uploadADatasetStore"].import)
    // The allowed file sizes and how mush data should be processed and viewed by default
    const {uploading} = useAppSelector(state => state["applicationStore"].clientConfig)

    // The user can either define their own schema or use a pre-existing one, the one to use to show the data in the table
    // with depends on what the user has selected in the UI
    let schema : ImportedFileSchema[]  = (selectedTab === "existing" && selectedOption?.tag?.definition?.schema?.table?.fields) ? selectedOption.tag.definition.schema.table.fields : guessedSchema

    // If the user has selected to use an existing schema for a dataset then since TRAC is case-insensitive the chosen
    // schema field names could not match the case of the loaded dataset. We added a property called jsonName in the
    // setSelectedSchemaOption function that is the actual name in the data.
    if (selectedTab === "existing") {

        schema = schema.map(field => {

            const newField = {...field}

            if (newField.jsonName) {
                newField.fieldName = newField.jsonName
            }

            return newField
        })
    }

    return (
        <React.Fragment>
            {status === "pending" &&
                <Loading/>
            }
            {status === "succeeded" && data && schema &&
                <React.Fragment>
                    <HeaderTitle type={"h3"} text={uploading.data.processing.maximumRowsToShowInSample === data.length && (!fileInfo?.numberOfRows || fileInfo.numberOfRows >  data.length) ? "Sample of imported data" : "Imported data"}/>

                    <Table className={"pt-2 pb-4"}
                           data={data}
                           isTracData={true}
                           schema={schema}
                           showHeader={false}/>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

export default SelectedDataInTable;