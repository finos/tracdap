/**
 * A component that performs some API calls to TRAC when the application loads and stores the results in the
 * application store. This can't be done in App.tsx as it needs to be within the Provider component in order to use
 * the Redux store.
 *
 * @module
 * @category Component
 */

import {getSetupItems} from "../scenes/ApplicationSetupScene/store/applicationSetupStore";
import {getPlatformInfo, getTenants, getUserNames} from "../utils/utils_trac_api";
import {isThemesList} from "../utils/utils_trac_type_chckers";
import {setThemeColours, showToast} from "../utils/utils_general";
import {setClientConfig, setLogin, setPlatformInfo, setTenantOptions, setUserNames} from "../store/applicationStore";
import {useAppDispatch, useAppSelector} from "../../types/types_hooks";
import {useEffect} from "react";

export const OnLoad = (): null => {

    // Set up the method used to update the redux store
    const dispatch = useAppDispatch()

    // Get what we need from the store
    const {"trac-tenant": tenant, "trac-theme": theme} = useAppSelector(state => state["applicationStore"].cookies)
    const {searchAsOf} = useAppSelector(state => state["applicationStore"].tracApi)

    useEffect(() => {

        /**
         * A hook that runs when the component mounts and saves information
         * from the TRAC authorisation cookies about who is logged in to root application store.
         */
        dispatch(setLogin())

        /**
         * A hook that runs when the component mounts which gets information about the available tenants and turns
         * this into a set of options for the user.
         */
        getTenants().then(response => dispatch(setTenantOptions(response))).catch(error => {

            showToast("error", {message: "Loading the available tenants failed", details: typeof error === "string" ? error : undefined}, "getTenants/rejected")
            console.error(error)
        });

        /**
         * A hook that runs when the component mounts that gets information about the platform, for example the
         * name of the platform and whether it is production
         */
        getPlatformInfo().then(response => dispatch(setPlatformInfo(response))).catch(error => {

            showToast("error", {message: "Loading the platform information failed", details: typeof error === "string" ? error : undefined}, "getPlatformInfo/rejected")
            console.error(error)
        });

        /**
         * A hook that runs when the component mounts and loads a json with the client specific parameters. This
         * is done so that details such as the model repositories can be dynamically updated without the need to
         * rebuild the application.
         */
        fetch("./static/client-config.json")
            .then(res => res.json())
            .then(clientConfig => dispatch(setClientConfig(clientConfig)))
            .catch(error => {
                showToast("error", {message: "Loading the client configuration file failed", details: error}, "getClientConfig/rejected")
                console.error(error);
            })

    }, [dispatch])

    /**
     * A hook that sets the theme colours to use based on what is in the application store when
     * the application is mounted. The "deps" argument says never run this on any render other
     * than the first.
     */
    useEffect(() => {

        console.log(`LOG :: Setting theme`)
        if (isThemesList(theme)) setThemeColours(theme)

    }, [theme])

    /**
     * A hook that gets data from TRAC needed to make the application run when the component mounts.
     * The "deps" argument says never run this on any render other than if the tenant changes. These
     * datasets are owned by the ApplicationSetupScene as they can be created and edited there, so we
     */
    useEffect(() => {

        // Get the data required from TRAC
        console.log(`LOG :: Getting data for application`)
        dispatch(getSetupItems())

        console.log(`LOG :: Getting list of users`)
        if (tenant) getUserNames(searchAsOf, tenant).then(response => dispatch(setUserNames(response))).catch(error => {

            showToast("error", {message: "Loading a list of users failed", details: typeof error === "string" ? error : undefined}, "getUserNames/rejected")
            console.error(error)
        });

    }, [dispatch, tenant, searchAsOf])

    return null
};