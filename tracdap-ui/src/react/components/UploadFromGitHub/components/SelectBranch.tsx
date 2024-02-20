/**
 * A component that shows a list of branches for the user to select to load from. If the platform info says
 * that this is a production instance of TRAC then only the main repo branch will be available.
 *
 * @module SelectBranch
 * @category Component
 */

import {Button} from "../../Button";
import Col from "react-bootstrap/Col";
import {getBranch, getRepository, setBranch, type UploadFromGitHubStoreState} from "../store/uploadFromGitHubStore";
import {HeaderTitle} from "../../HeaderTitle";
import {Icon} from "../../Icon";
import PropTypes from "prop-types";
import React, {useEffect, useRef, useState} from "react";
import Row from "react-bootstrap/Row";
import {SelectOption} from "../../SelectOption";
import {TextBlock} from "../../TextBlock";
import {tracdap as trac} from "@finos/tracdap-web-api";
import {useAppDispatch, useAppSelector} from "../../../../types/types_hooks";

/**
 * An interface for the props of the SelectBranch component.
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

export const SelectBranch = (props: Props) => {

    const {storeKey, uploadType} = props

    // Get what we need from the store
    const {selectedOption} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].repositories)
    const {branches, status, details} = useAppSelector(state => state["uploadFromGitHubStore"].uses[storeKey].repository)

    // We are going to allow the user to toggle between showing and hiding the disabled branches as there could be loads
    // and only a few that are available
    const [hideDisabledOptions, setHideDisabledOptions] = useState(true)

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    /**
     * A function that runs when the user clicks to toggle the branch option filtering to hide/show disabled
     * branches.
     */
    function toggleHideDisabledOptions(): void {

        setHideDisabledOptions(prevState => !prevState)
    }

    /**
     * Awesomeness. A hook that runs every render except when the component mounts. This starts with this ref.
     */
    const componentIsMounting = useRef(true)

    /**
     * A hook that runs when the selected branch changes, this goes and gets the information about the branch.
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
            if (branches.selectedOption) dispatch(getBranch({storeKey}))

        } else {
            componentIsMounting.current = false
        }

    }, [branches.selectedOption, dispatch, storeKey])

    return (

        <React.Fragment>
            {/*Show once a call has been made at least once */}
            {status !== "idle" && details != null &&

                <React.Fragment>

                    <HeaderTitle type={"h3"} text={"Select your repository branch"}/>

                    <TextBlock>
                        Below is a list of your repository&apos;s branches. You are only allowed to upload {uploadType}s from
                        the &apos;{selectedOption != null ? selectedOption.details.defaultBranch : "main"}&apos; branch when
                        uploading into the production environment. This means that all changes must be pulled into the
                        default branch, reviewed and approved. All other branches will be disabled for loads into the
                        production environment.
                    </TextBlock>

                    <Row className={"pb-3"}>
                        <Col xs={9} md={7} lg={6} xl={5}>
                            <SelectOption basicType={trac.STRING}
                                          hideDisabledOptions={hideDisabledOptions}
                                          isLoading={Boolean(status === "pending")}
                                          name={storeKey}
                                          onChange={setBranch}
                                          options={branches.options}
                                          placeHolderText={Boolean(status === "pending") ? "Loading..." : undefined}
                                          validateOnMount={false}
                                          value={branches.selectedOption}
                            />
                        </Col>

                        <Col xs={3} md={5} lg={6} xl={7} className={"my-auto"}>
                            <Button ariaLabel={"Refresh branches"}
                                    className={"m-0 p-0 fs-4"}
                                    id={"refresh"}
                                    name={storeKey}
                                // The list of branches is part of the repository definition, so
                                // we need to re-fetch that to update the list of branches
                                    onClick={getRepository}
                                    variant={"link"}
                            >
                                <Icon
                                    ariaLabel={false}
                                    icon={"bi-arrow-clockwise"}
                                    tooltip={"Refresh branches"}
                                />
                            </Button>

                            <Button ariaLabel={"Toggle disabled repositories"}
                                    className={"my-0 me-0 ms-3 p-0 fs-4"}
                                    isDispatched={false}
                                    onClick={toggleHideDisabledOptions}
                                    variant={"link"}
                            >
                                <Icon
                                    ariaLabel={false}
                                    icon={!hideDisabledOptions ? "bi-list-task" : "bi-funnel"}
                                    tooltip={hideDisabledOptions ? "Show disabled branches" : "Hide disabled branches"}
                                />
                            </Button>
                        </Col>
                    </Row>

                </React.Fragment>
            }
        </React.Fragment>
    )
};

SelectBranch.propTypes = {

    storeKey: PropTypes.string.isRequired,
    uploadType: PropTypes.oneOf(["model", "schema"]).isRequired
};