import {DataViewer} from "./components/DataViewer";
import {FlowViewer} from "./components/FlowViewer";
import {JobViewer} from "./components/JobViewer";
import {ModelViewer} from "./components/ModelViewer";
import {ObjectTypesString} from "../../../types/types_general";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {SchemaViewer} from "./components/SchemaViewer";

/**
 * This scene allows the user to see a summary of a particular version of an object stored in TRAC. This is the same
 * as the object summaries shown in modals but in its own page rather than a popup.
 */

type Props = {

    /**
     * The type of object being viewed e.g. "DATA", "MODEL"
     */
    objectTypeAsString: ObjectTypesString
    /**
     * The main title for the page, this is set in the Menu config.
     */
    title: string
};

const ObjectSummaryScene = (props: Props) => {

    console.log("Rendering ObjectSummaryScene")

    const {title, objectTypeAsString} = props;

    return (

        <React.Fragment>

            <SceneTitle text={title}/>

            {objectTypeAsString === "DATA" &&
                <DataViewer getTagFromUrl={true}/>
            }
            {objectTypeAsString === "MODEL" &&
                <ModelViewer getTagFromUrl={true}/>
            }
            {objectTypeAsString === "SCHEMA" &&
                <SchemaViewer getTagFromUrl={true}/>
            }
            {objectTypeAsString === "JOB" &&
                <JobViewer getTagFromUrl={true}/>
            }
            {objectTypeAsString === "FLOW" &&
                <FlowViewer getTagFromUrl={true} storeKey={"searchResult"}/>
            }
            {objectTypeAsString === "CUSTOM" &&
                <span>Custom viewer is not configured</span>
            }
            {objectTypeAsString === "STORAGE" &&
                <span>Storage viewer is not configured</span>
            }
            {objectTypeAsString === "FILE" &&
                <span>File viewer is not configured</span>
            }
            {objectTypeAsString === "OBJECT_TYPE_NOT_SET" &&
                <span>Objects without a type can not be viewed</span>
            }

        </React.Fragment>
    )
};

export default ObjectSummaryScene;