/**
 * A component that shows a list of commits for the user to select to load from.
 *
 * @module SelectCommit
 * @category Component
 */

import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import {getBranch, getTree, setCommit, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {HeaderTitle} from "../../HeaderTitle";
import {Icon} from "../../Icon";
import PropTypes from "prop-types";
import {SelectOption} from "../../SelectOption";
import React, {useEffect, useRef} from "react";
import Row from "react-bootstrap/Row";
import {TextBlock} from "../../TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the SelectCommit component.
 */
export interface Props {

    /**
     * The key in the UploadFromGitHubStore to get the state for this component
     */
    storeKey: keyof UploadFromGitHubStoreState["uses"]
    /**
     * The type of file being selected, it is used in messages to the user.
     */
    uploadType: "model" | "schema"
}

export const SelectCommit = (props: Props) => {

    const {storeKey, uploadType} = props

    // Get what we need from the store
    const {commit, status} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].branch)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * Awesomeness. A hook that runs every render except when the component mounts. This starts with this ref.
     */
    const componentIsMounting = useRef(true)

    /**
     * A hook that runs when the selected commit changes, this goes and gets the information about the commit.
     * Note that we use componentIsMounting to prevent it from running when the components mounts, this is so we use
     * the values in the store rather than getting all the information again. We also do this because some functions
     * that get called in this component reset parts of the store, so if you allow them to run when the component
     * mounts you end up wiping parts of the state.
     */
    useEffect(() => {

        // Strictly only ever run this after the component mounts
        if (!componentIsMounting.current) {
            // The selected option is not really needed, but we want to trigger this
            // when the option changes
            if (commit.selectedOption) dispatch(getTree({storeKey}))
        } else {
            componentIsMounting.current = false
        }

    }, [commit.selectedOption, dispatch, storeKey])

    return (

        <React.Fragment>
            {/*Show once a call has been made at least once */}
            {status !== "idle" &&

                <React.Fragment>

                    <HeaderTitle type={"h3"} text={"Select your branch commit"}/>

                    <TextBlock>
                        Below is a list of your branch&apos;s commits that you can select to load a {uploadType} from. You are only
                        allowed to upload {uploadType}s from the latest commit or from commits that have specific tags or are
                        part of a release.
                    </TextBlock>

                    <Row className={"pb-3"}>
                        <Col xs={9} md={7} lg={6} xl={5}>
                            <SelectOption basicType={trac.STRING}
                                          isLoading={Boolean(status === "pending")}
                                          name={storeKey}
                                          onChange={setCommit}
                                          options={commit.options}
                                          placeHolderText={Boolean(status === "pending") ? "Loading..." : undefined}
                                          validateOnMount={false}
                                          value={commit.selectedOption}
                            />
                        </Col>

                        <Col xs={3} md={5} lg={6} xl={7} className={"my-auto"}>

                            <Button ariaLabel={"Refresh commits"}
                                    className={"m-0 p-0 fs-4"}
                                    id={"refresh"}
                                    name={storeKey}
                                    onClick={getBranch}
                                    variant={"link"}
                            >
                                <Icon ariaLabel={false}
                                      icon={"bi-arrow-clockwise"}
                                      tooltip={"Refresh commits"}
                                />
                            </Button>

                        </Col>
                    </Row>
                </React.Fragment>
            }
        </React.Fragment>
    )
};

SelectCommit.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};