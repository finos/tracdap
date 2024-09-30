
import React from "react";
import PropTypes from "prop-types";
import {SceneTitle} from "../../components/SceneTitle";
import OpenModalToSelectDataset from "./components/OpenModalToSelectDataset";
import {QueryBuilder} from "../../components/QueryBuilder/QueryBuilder";
import {FindInTrac} from "../../components/FindInTrac";
import {useAppSelector} from "../../../types/types_hooks";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {TextBlock} from "../../components/TextBlock";
import {HeaderTitle} from "../../components/HeaderTitle";
import {ObjectDetails} from "../../components/ObjectDetails";

/**
 * This scene allows the user to build and execute a SQL query on a dataset.
 *
 * Note that the scene uses the FindInTrac component which is used in multiple places in the application, a key is
 * used to load up the right values
 */

type Props = {

    /**
     * The title for the scene, this is set in config_menu.tsx.
     */
    title: string
}

const DataAnalyticsScene = (props: Props) => {

    console.log("Rendering DataAnalyticsScene")

    const {title} = props;

    // The object tags for the rows selected in the table
    const {tags} = useAppSelector(state => state["dataAnalyticsStore"].selectedData)

    return (

        <React.Fragment>

            <SceneTitle text={title}/>

            <TextBlock>
                This tool can be used to perform analytics on datasets stored in TRAC. You can use the button below to search for a dataset
                or find a job and select one of its outputs to query. The menu below allows you to build your own query, either
                by using the user interface or by unlocking the editor and manually writing your own SQL query.
            </TextBlock>

            <OpenModalToSelectDataset/>

            {tags.length > 0 &&
                <React.Fragment>
                    <HeaderTitle type={"h3"} text={"Selected dataset"}/>
                    <ObjectDetails bordered={false} metadata={tags[0]} striped={true}/>
                    <QueryBuilder storeKey={"dataAnalytics"}
                                  metadata={tags[0]}
                    />
                </React.Fragment>
            }

        </React.Fragment>
    )
};

export default DataAnalyticsScene;