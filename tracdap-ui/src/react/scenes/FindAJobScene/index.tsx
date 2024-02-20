import {FindInTrac} from "../../components/FindInTrac";
import React from "react";
import PropTypes from "prop-types";
import {SceneTitle} from "../../components/SceneTitle";
import {TextBlock} from "../../components/TextBlock";

/**
 * This scene allows the user to find a job using various search criteria and then list the search results in a
 * table. The user can then select which job they want to load into the ObjectSummaryScene or rerun in the RunAFlowScene.
 *
 * Note that the scene uses the FindInTrac component which is used in multiple places in the application, a key is
 * used to load up the right values
 */

type Props = {

    /**
     * The main title for the page, this is set in the Menu config.
     */
    title: string,
};

const FindAJobScene = (props: Props) => {

    console.log("Rendering FindAJobScene")

    const {title} = props

    return (

        <React.Fragment>

            <SceneTitle text={title}/>

            <TextBlock className={"mt-3 mb-2 "}>
                You can use the menu below to find jobs that have been run or that are in progress.
                If you select a job then you can see summary information about it or load up the full details.
            </TextBlock>

            <FindInTrac storeKey={"findAJob"}/>

        </React.Fragment>

    )
};

export default FindAJobScene;

FindAJobScene.propTypes = {

    title: PropTypes.string.isRequired
};