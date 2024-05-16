/**
 * A component that shows a burger icon that when clicked on shows a toolbar of options to the user to control a menu.
 * @module Toolbar
 * @category RunAModelScene component
 */

import {ActionCreatorWithoutPayload, ActionCreatorWithPayload} from "@reduxjs/toolkit";
import {Burger} from "../../../components/Burger";
import {BusinessSegmentsStoreState} from "../../../components/BusinessSegments/businessSegmentsStore";
import Dropdown from "react-bootstrap/Dropdown";
import {Icon} from "../../../components/Icon";
import {ListsOrTabs} from "../../../../types/types_general";
import React, {memo, useState} from "react";
import PropTypes from "prop-types";
import {useAppDispatch} from "../../../../types/types_hooks";

export interface Props {

    /**
     * Whether the dropdown menu should be disabled. For example while API calls are being made.
     */
    disabled: boolean
    /**
     * Whether to show the menu layout that this toolbar controls as tabs or in a single list with headers.
     */
    listsOrTabs?: ListsOrTabs
    /**
     * The menu type that the toolbar is controlling.
     */
    name: "inputs" | "models" | "parameters"
    /**
     * A function to run that updates the option labels in the {@link SelectOption} components in the menu
     * that this toolbar controls.
     */
    onChangeLabel?: ActionCreatorWithPayload<{ name: "inputs" | "models", id: "showObjectId" | "showVersions" | "showCreatedDate" | "showUpdatedDate" }>
    /**
     * A function to run that updates the options in the menu. This has multiple signatures depending on the
     * menu being controlled.
     */
    onRefresh?: Function
    /**
     * A function to run to show information related to a selected item in the menu e.g. the selected model.
     */
    onShowInfo?: () => void
    /**
     * A function to run that updates the 'showKeysInsteadOfLabels' prop value.
     */
    onShowKeys?: ActionCreatorWithPayload<{ name: "inputs" | "parameters" }>
    /**
     * A function to run that updates the 'listsOrTabs' prop value.
     */
    onShowTabs?: ActionCreatorWithoutPayload,
    /**
     * Whether the user has selected to view the created date in the option labels in the {@link SelectOption} components in the
     * menu that this toolbar controls. For example when showing a list of input datasets.
     */
    showCreatedDate?: boolean
    /**
     * Whether to show the menu items that this toolbar controls with the key are the header rather than the associated label.
     */
    showKeysInsteadOfLabels?: boolean
    /**
     * Whether the user has selected to view the object ID in the option labels in the {@link SelectOption} components in the menu
     * that this toolbar controls. For example when showing a list of input datasets.
     */
    showObjectId?: boolean
    /**
     * Whether the user has selected to view the update date in the option labels in the {@link SelectOption} components in the menu
     * that this toolbar controls. For example when showing a list of input datasets.
     */
    showUpdatedDate?: boolean
    /**
     * Whether the user has selected to view the object versions (tag and object) in the option labels in the {@link SelectOption}
     * components in the menu that this toolbar controls. For example when showing a list of input datasets.
     */
    showVersions?: boolean
    /**
     *  The key in the BusinessSegmentsStore to use to get the business segments selected by the user for whatever is in
     *  the toolbar.
     */
    storeKey?: keyof BusinessSegmentsStoreState["uses"]
}

const ToolbarInner = (props: Props) => {

    const {
        name,
        disabled,
        onShowInfo,
        onRefresh,
        onChangeLabel,
        onShowKeys,
        onShowTabs,
        showCreatedDate,
        showKeysInsteadOfLabels,
        showObjectId,
        listsOrTabs,
        showUpdatedDate,
        showVersions,
        storeKey
    } = props

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Whether the submenu is open or not
    const [burgerOpen, setBurgerOpen] = useState<boolean>(false)

    // Whether to show the toolbar, if there are no functions to run we don't show it
    const show = Boolean(onShowInfo || onRefresh || onChangeLabel || onShowKeys || onShowTabs)

    return (
        <React.Fragment>
            {show &&
                <Dropdown className={``}
                          drop={"end"}
                          onToggle={isOpen => setBurgerOpen(isOpen)}
                >
                    <Dropdown.Toggle bsPrefix={"p-0"}
                                     className={`no-halo`}
                                     id={"object-menu-button"}
                                     size={"sm"}
                                     title={"Dropdown button"}
                                     variant={"link"}
                    >
                        <Burger ariaLabel={"Menu icon"} open={burgerOpen} size={"md"}/>
                    </Dropdown.Toggle>

                    <Dropdown.Menu className={"py-2"}>

                        {onShowKeys && (name === "inputs" || name === "parameters") &&
                            <Dropdown.Item className={"fs-8 px-3"}
                                           disabled={disabled}
                                           onClick={() => dispatch(onShowKeys({name}))}
                                           size={"sm"}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={showKeysInsteadOfLabels ? "bi-type" : "bi-link"}
                                />
                                <span>Show {showKeysInsteadOfLabels ? "labels" : "keys"}</span>
                            </Dropdown.Item>
                        }

                        {onShowTabs &&

                            <Dropdown.Item className={"fs-8 px-3"}
                                           disabled={disabled}
                                           onClick={() => dispatch(onShowTabs())}
                                           size={"sm"}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={listsOrTabs === "lists" ? "bi-segmented-nav" : "bi-list-ul"}
                                />
                                <span>Show {listsOrTabs === "lists" ? "in tabs" : "as list"}</span>
                            </Dropdown.Item>
                        }

                        {(onShowKeys || onShowTabs) && (onShowInfo || onRefresh || onChangeLabel) &&
                            <div className="dropdown-divider"/>
                        }

                        {onShowInfo &&
                            <Dropdown.Item className={"fs-8 px-3"}
                                           disabled={disabled}
                                           onClick={onShowInfo}
                                           size={"sm"}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-info-circle"}
                                />
                                <span>Info</span>
                            </Dropdown.Item>
                        }

                        {onRefresh &&
                            <Dropdown.Item className={"fs-8 px-3"}
                                           disabled={disabled}
                                           onClick={() => {
                                               if (storeKey) {
                                                   dispatch(onRefresh({storeKey: storeKey}))
                                               } else {
                                                   dispatch(onRefresh())
                                               }
                                           }}
                                           size={"sm"}
                            >
                                <Icon ariaLabel={false}
                                      className={"pe-2"}
                                      icon={"bi-arrow-clockwise"}
                                /><span>Refresh</span>
                            </Dropdown.Item>
                        }

                        {onChangeLabel && (name == "inputs" ||  name == "models") &&
                            <React.Fragment>
                                <Dropdown.Item className={"fs-8 px-3"}
                                               disabled={disabled}
                                               onClick={() => dispatch(onChangeLabel({id: "showObjectId", name}))}
                                               size={"sm"}
                                >
                                    <Icon ariaLabel={false}
                                          className={"pe-2"}
                                          icon={"bi-key"}
                                    />
                                    <span>{showVersions === undefined ? "Toggle" : (showObjectId ? "Hide" : "Show")} object ID</span>
                                </Dropdown.Item>
                                <Dropdown.Item className={"fs-8 px-3"}
                                               disabled={disabled}
                                               onClick={() => dispatch(onChangeLabel({id: "showVersions", name}))}
                                               size={"sm"}
                                >
                                    <Icon ariaLabel={false}
                                          className={"pe-2"}
                                          icon={"bi-diagram-3"}
                                    />
                                    <span>{showVersions === undefined ? "Toggle" : (showVersions ? "Hide" : "Show")} versions</span>
                                </Dropdown.Item>
                                <Dropdown.Item className={"fs-8 px-3"}
                                               disabled={disabled}
                                               onClick={() => dispatch(onChangeLabel({id: "showUpdatedDate", name}))}
                                               size={"sm"}
                                >
                                    <Icon ariaLabel={false}
                                          className={"pe-2"}
                                          icon={"bi-calendar-date"}
                                    />
                                    <span>{showUpdatedDate === undefined ? "Toggle" : (showUpdatedDate ? "Hide" : "Show")} Updated dates</span>
                                </Dropdown.Item>
                                <Dropdown.Item className={"fs-8 px-3"}
                                               disabled={disabled}
                                               onClick={() => dispatch(onChangeLabel({id: "showCreatedDate", name}))}
                                               size={"sm"}
                                >
                                    <Icon ariaLabel={false}
                                          className={"pe-2"}
                                          icon={"bi-calendar-day"}
                                    />
                                    <span>{showCreatedDate === undefined ? "Toggle" : (showCreatedDate ? "Hide" : "Show")} created dates</span>
                                </Dropdown.Item>
                            </React.Fragment>
                        }

                    </Dropdown.Menu>
                </Dropdown>
            }
        </React.Fragment>
    )
};

ToolbarInner.propTypes = {

    disabled: PropTypes.bool.isRequired,
    listsOrTabs: PropTypes.oneOf(["lists", "tabs"]),
    name: PropTypes.oneOf(["inputs", "models", "parameters"]).isRequired,
    onChangeLabel: PropTypes.func,
    onRefresh: PropTypes.func,
    onShowInfo: PropTypes.func,
    onShowKeys: PropTypes.func,
    onShowTabs: PropTypes.func,
    showCreatedDate: PropTypes.bool,
    showKeysInsteadOfLabels: PropTypes.bool,
    showObjectId: PropTypes.bool,
    showUpdatedDate: PropTypes.bool,
    showVersions: PropTypes.bool,
    storeKey: PropTypes.string
};

export const Toolbar = memo(ToolbarInner);