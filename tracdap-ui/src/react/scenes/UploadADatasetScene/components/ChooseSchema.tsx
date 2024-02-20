import {HeaderTitle} from "../../../components/HeaderTitle";
import React, {useCallback} from "react";
import {setTab} from "../store/uploadADatasetStore";
import Tab from "react-bootstrap/Tab";
import Tabs from "react-bootstrap/Tabs";
import {TextBlock} from "../../../components/TextBlock";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";
import SuggestedSchemaTab from "./SuggestedSchemaTab";
import ExistingSchemaTab from "./ExistingSchemaTab";

/**
 * A component that allows the user to search TRAC for a schema that can be used for the selected dataset or edit the
 * one guessed at by the FileImport.
 */
const ChooseSchema = () => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {status} = useAppSelector(state => state["uploadADatasetStore"].import)
    const {selectedTab, options} = useAppSelector(state => state["uploadADatasetStore"].existingSchemas)

    console.log("Rendering ChooseSchema")

    /**
     * A function that runs when the user changes the tab between the suggested schema and schemas already in TRAC. We
     * need to control the tab selected because we need to know whether to load the data with the suggested or
     * pre-existing schema.
     * @param key - The key of the tab selected.
     */
    const handleTabChange = useCallback((key: string | null): void => {
        if (key) dispatch(setTab(key))
    }, [dispatch])

    return (
        <React.Fragment>

            {status === "succeeded" &&
                <React.Fragment>

                    <HeaderTitle type={"h3"} text={"Edit dataset schema"}/>

                    <TextBlock>
                        All datasets in TRAC have a schema that details information about each variable. You can either
                        use an existing schema or edit a schema that has been guessed for you.
                    </TextBlock>

                    <Tabs defaultActiveKey={"suggested"}
                          id={"select-schema"}
                          activeKey={selectedTab}
                          onSelect={handleTabChange}
                    >

                        <Tab eventKey="suggested" title="Suggested schema"
                             className={"tab-content-bordered pt-5 pb-5 mb-5"}>

                            <SuggestedSchemaTab/>
                        </Tab>

                        <Tab className={"tab-content-bordered pt-5 pb-5 mb-5"}
                             eventKey="existing"
                             title={`Use an existing schema (${options.length})`}
                        >

                            <ExistingSchemaTab/>

                        </Tab>
                    </Tabs>

                </React.Fragment>
            }

        </React.Fragment>
    )
};

export default ChooseSchema;