/**
 * This scene allows the user to set up the objects in TRAC required by the user interface to run. Its purpose
 * is to avoid the application owner having to do any back end API calls to get started. For example, it allows
 * the user to set up the business segments dataset that is used to by the application to perform searches.
 * @module ApplicationSetupScene
 * @category Scene
 */

import {Alert} from "../../components/Alert";
import {DatasetSummaryTable} from "./components/DatasetSummaryTable";
import {Editor} from "./components/Editor";
import {Loading} from "../../components/Loading";
import PropTypes from "prop-types";
import React from "react";
import {SceneTitle} from "../../components/SceneTitle";
import {TextBlock} from "../../components/TextBlock";
import {useAppSelector} from "../../../types/types_hooks";

/**
 * An interface for the props of the ApplicationSetupScene index component.
 */
export interface Props {

    /**
     * The main title for the page, this is set in the {@link MenuConfig}.
     */
    title: string
}

const ApplicationSetupScene = (props: Props) => {

    console.log("Rendering ApplicationSetupScene")

    const {title} = props

    // Get what we need from the store
    const {status} = useAppSelector(state => state["applicationSetupStore"].tracItems.getSetupItems)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    return (

        <React.Fragment>

            <SceneTitle text={title}/>

            {/*The OnLoad component gets the datasets from TRAC but if this scene is the first page the user loads we still*/}
            {/*need to show a loading icon while that happens*/}
            {status === "pending" &&
                <Loading/>
            }

            {status === "succeeded" &&
                <React.Fragment>

                    <TextBlock>
                        This page allows you to add the datasets to TRAC that are required for the user
                        interface to function properly. While setting them up is a one off activity you are able to edit
                        some of the items should your requirements change in the future, for example if the business
                        segmentation used by the application needs to be extended.
                    </TextBlock>

                    <TextBlock>
                        The benefit of storing these items in TRAC rather than defining them in the user interface code
                        is that updating them does not require a new release of the application. It also makes the
                        process more managed, transparent and means anyone can perform these activities not just a back
                        end support team.
                    </TextBlock>

                    {searchAsOf &&
                        <Alert variant={"warning"} >
                            You will not be able to create or edit these datasets in time travel mode.
                        </Alert>
                    }

                    <DatasetSummaryTable/>

                    <Editor/>

                </React.Fragment>
            }

        </React.Fragment>
    )
};

ApplicationSetupScene.propTypes = {

    title: PropTypes.string.isRequired
};

// This additional export is needed to be able to load this scene lazily in config_menu.tsx
export default ApplicationSetupScene;
