import {Alert} from "../../../components/Alert";
import Col from "react-bootstrap/Col";
import {General} from "../../../../config/config_general";
import {HeaderTitle} from "../../../components/HeaderTitle";
import {MetadataViewer} from "../../../components/MetadataViewer/MetadataViewer";
import React from "react";
import Row from "react-bootstrap/Row";
import {SchemaFieldsTable} from "../../../components/SchemaFieldsTable/SchemaFieldsTable";
import {SelectOption} from "../../../components/SelectOption";
import {setSelectedSchemaOption} from "../store/uploadADatasetStore";
import {ShowHideDetails} from "../../../components/ShowHideDetails";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppSelector} from "../../../../types/types_hooks";

// Some Bootstrap grid layouts
const lgGrid = {span: 10, offset: 1}
const xlGrid = {span: 8, offset: 2}

/**
 * A component that allows the user to choose an existing schema that matches that of a dataset loaded from a local file.
 */
const ExistingSchemaTab = () => {

    // Get what we need from the store
    const {isExistingSchemaTheSame} = useAppSelector(state => state["uploadADatasetStore"].alreadyInTrac)
    const {options, selectedOption} = useAppSelector(state => state["uploadADatasetStore"].existingSchemas)

    return (

        <Row>
            <Col xs={12} lg={lgGrid} xl={xlGrid} className={"mb-2"}>
                <SelectOption basicType={trac.BasicType.STRING}
                              isClearable={true}
                              labelText={"Please select schema to apply to this dataset"}
                              mustValidate={false}
                              onChange={setSelectedSchemaOption}
                              options={options}
                              showValidationMessage={false}
                              validateOnMount={false}
                              validationChecked={false}
                              value={selectedOption}
                />

                {options.length === 0 &&
                    <Alert className={"mt-4"} variant={"warning"}>
                        <div>
                            There are no schemas available that match the loaded dataset, please use
                            the suggested schema.
                        </div>
                    </Alert>
                }

                {!General.loading.allowCopies.data && isExistingSchemaTheSame &&
                    <Alert className={"mt-4 mb-1"} variant={"warning"}>
                        <div>
                            The chosen schema&apos;s names and types are identical to the version stored
                            in the dataset already loaded into TRAC. If you need to edit the field
                            names or types then that can be done by editing the suggested schema.
                        </div>
                    </Alert>
                }
            </Col>

            {selectedOption && selectedOption.tag.definition?.schema?.table?.fields &&

                <Col xs={12} lg={lgGrid} xl={xlGrid}>

                    <ShowHideDetails linkText={"metadata"} classNameOuter={"mt-4 mb-3 pb-3"} classNameInner={"pt-2 pb-4"}>
                        <MetadataViewer metadata={selectedOption.tag}/>
                    </ShowHideDetails>

                    <SchemaFieldsTable className={"mt-2"}
                                       fields={selectedOption.tag.definition?.schema?.table?.fields}>
                        <HeaderTitle type={"h4"} text={"Schema"} outerClassName={"my-0"}/>
                    </SchemaFieldsTable>
                </Col>
            }
        </Row>
    )
};

export default ExistingSchemaTab;