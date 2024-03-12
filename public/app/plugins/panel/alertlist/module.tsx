import React from 'react';

import { DataSourceInstanceSettings, PanelPlugin } from '@grafana/data';
import { config } from '@grafana/runtime';
import { Button, Stack, TagsInput } from '@grafana/ui';
import { OldFolderPicker } from 'app/core/components/Select/OldFolderPicker';
import {
  ALL_FOLDER,
  GENERAL_FOLDER,
  ReadonlyFolderPicker,
} from 'app/core/components/Select/ReadonlyFolderPicker/ReadonlyFolderPicker';
import { DataSourcePicker } from 'app/features/datasources/components/picker/DataSourcePicker';
import { PermissionLevelString } from 'app/types';

import { GRAFANA_DATASOURCE_NAME } from '../../../features/alerting/unified/utils/datasource';

import { AlertList } from './AlertList';
import { alertListPanelMigrationHandler } from './AlertListMigrationHandler';
import { GroupBy } from './GroupByWithLoading';
import { UnifiedAlertListPanel } from './UnifiedAlertList';
import { AlertListSuggestionsSupplier } from './suggestions';
import { AlertListOptions, GroupMode, ShowOption, SortOrder, UnifiedAlertListOptions, ViewMode } from './types';

function showIfCurrentState(options: AlertListOptions) {
  return options.showOptions === ShowOption.Current;
}

const alertList = new PanelPlugin<AlertListOptions>(AlertList)
  .setPanelOptions((builder) => {
    builder
      .addSelect({
        name: 'Show',
        path: 'showOptions',
        settings: {
          options: [
            { label: 'Current state', value: ShowOption.Current },
            { label: 'Recent state changes', value: ShowOption.RecentChanges },
          ],
        },
        defaultValue: ShowOption.Current,
        category: ['Options'],
      })
      .addNumberInput({
        name: 'Max items',
        path: 'maxItems',
        defaultValue: 10,
        category: ['Options'],
      })
      .addSelect({
        name: 'Sort order',
        path: 'sortOrder',
        settings: {
          options: [
            { label: 'Alphabetical (asc)', value: SortOrder.AlphaAsc },
            { label: 'Alphabetical (desc)', value: SortOrder.AlphaDesc },
            { label: 'Importance', value: SortOrder.Importance },
            { label: 'Time (asc)', value: SortOrder.TimeAsc },
            { label: 'Time (desc)', value: SortOrder.TimeDesc },
          ],
        },
        defaultValue: SortOrder.AlphaAsc,
        category: ['Options'],
      })
      .addBooleanSwitch({
        path: 'dashboardAlerts',
        name: 'Alerts from this dashboard',
        defaultValue: false,
        category: ['Options'],
      })
      .addTextInput({
        path: 'alertName',
        name: 'Alert name',
        defaultValue: '',
        category: ['Filter'],
        showIf: showIfCurrentState,
      })
      .addTextInput({
        path: 'dashboardTitle',
        name: 'Dashboard title',
        defaultValue: '',
        category: ['Filter'],
        showIf: showIfCurrentState,
      })
      .addCustomEditor({
        path: 'folderId',
        name: 'Folder',
        id: 'folderId',
        defaultValue: null,
        editor: function RenderFolderPicker({ value, onChange }) {
          return (
            <ReadonlyFolderPicker
              initialFolderId={value}
              onChange={(folder) => onChange(folder?.id)}
              extraFolders={[ALL_FOLDER, GENERAL_FOLDER]}
            />
          );
        },
        category: ['Filter'],
        showIf: showIfCurrentState,
      })
      .addCustomEditor({
        id: 'tags',
        path: 'tags',
        name: 'Tags',
        description: '',
        defaultValue: [],
        editor(props) {
          return <TagsInput tags={props.value} onChange={props.onChange} />;
        },
        category: ['Filter'],
        showIf: showIfCurrentState,
      })
      .addBooleanSwitch({
        path: 'stateFilter.ok',
        name: 'Ok',
        defaultValue: false,
        category: ['State filter'],
        showIf: showIfCurrentState,
      })
      .addBooleanSwitch({
        path: 'stateFilter.paused',
        name: 'Paused',
        defaultValue: false,
        category: ['State filter'],
        showIf: showIfCurrentState,
      })
      .addBooleanSwitch({
        path: 'stateFilter.no_data',
        name: 'No data',
        defaultValue: false,
        category: ['State filter'],
        showIf: showIfCurrentState,
      })
      .addBooleanSwitch({
        path: 'stateFilter.execution_error',
        name: 'Execution error',
        defaultValue: false,
        category: ['State filter'],
        showIf: showIfCurrentState,
      })
      .addBooleanSwitch({
        path: 'stateFilter.alerting',
        name: 'Alerting',
        defaultValue: false,
        category: ['State filter'],
        showIf: showIfCurrentState,
      })
      .addBooleanSwitch({
        path: 'stateFilter.pending',
        name: 'Pending',
        defaultValue: false,
        category: ['State filter'],
        showIf: showIfCurrentState,
      });
  })
  .setMigrationHandler(alertListPanelMigrationHandler)
  .setSuggestionsSupplier(new AlertListSuggestionsSupplier());

const unifiedAlertList = new PanelPlugin<UnifiedAlertListOptions>(UnifiedAlertListPanel).setPanelOptions((builder) => {
  builder
    .addRadio({
      path: 'viewMode',
      name: 'View mode',
      description: 'Toggle between list view and stat view',
      defaultValue: ViewMode.List,
      settings: {
        options: [
          { label: 'List', value: ViewMode.List },
          { label: 'Stat', value: ViewMode.Stat },
        ],
      },
      category: ['Options'],
    })
    .addRadio({
      path: 'groupMode',
      name: 'Group mode',
      description: 'How alert instances should be grouped',
      defaultValue: GroupMode.Default,
      settings: {
        options: [
          { value: GroupMode.Default, label: 'Default grouping' },
          { value: GroupMode.Custom, label: 'Custom grouping' },
        ],
      },
      category: ['Options'],
    })
    .addCustomEditor({
      path: 'groupBy',
      name: 'Group by',
      description: 'Filter alerts using label querying',
      id: 'groupBy',
      defaultValue: [],
      showIf: (options) => options.groupMode === GroupMode.Custom,
      category: ['Options'],
      editor: (props) => {
        return (
          <GroupBy
            id={props.id ?? 'groupBy'}
            defaultValue={props.value.map((value: string) => ({ label: value, value }))}
            onChange={props.onChange}
            dataSource={props.context.options.datasource}
          />
        );
      },
    })
    .addNumberInput({
      name: 'Max items',
      path: 'maxItems',
      description: 'Maximum alerts to display',
      defaultValue: 20,
      category: ['Options'],
    })
    .addSelect({
      name: 'Sort order',
      path: 'sortOrder',
      description: 'Sort order of alerts and alert instances',
      settings: {
        options: [
          { label: 'Alphabetical (asc)', value: SortOrder.AlphaAsc },
          { label: 'Alphabetical (desc)', value: SortOrder.AlphaDesc },
          { label: 'Importance', value: SortOrder.Importance },
          { label: 'Time (asc)', value: SortOrder.TimeAsc },
          { label: 'Time (desc)', value: SortOrder.TimeDesc },
        ],
      },
      defaultValue: SortOrder.AlphaAsc,
      category: ['Options'],
    })
    .addBooleanSwitch({
      path: 'dashboardAlerts',
      name: 'Alerts linked to this dashboard',
      description: 'Only show alerts linked to this dashboard',
      defaultValue: false,
      category: ['Options'],
    })
    .addTextInput({
      path: 'alertName',
      name: 'Alert name',
      description: 'Filter for alerts containing this text',
      defaultValue: '',
      category: ['Filter'],
    })
    .addTextInput({
      path: 'alertInstanceLabelFilter',
      name: 'Alert instance label',
      description: 'Filter alert instances using label querying, ex: {severity="critical", instance=~"cluster-us-.+"}',
      defaultValue: '',
      category: ['Filter'],
    })
    .addCustomEditor({
      path: 'datasource',
      name: 'Datasource',
      description: 'Filter from alert source',
      id: 'datasource',
      defaultValue: null,
      editor: function RenderDatasourcePicker(props) {
        return (
          <Stack gap={1}>
            <DataSourcePicker
              {...props}
              type={['prometheus', 'loki', 'grafana']}
              noDefault
              current={props.value}
              onChange={(ds: DataSourceInstanceSettings) => props.onChange(ds.name)}
            />
            <Button variant="secondary" onClick={() => props.onChange(null)}>
              Clear
            </Button>
          </Stack>
        );
      },
      category: ['Filter'],
    })
    .addCustomEditor({
      showIf: (options) => options.datasource === GRAFANA_DATASOURCE_NAME || !Boolean(options.datasource),
      path: 'folder',
      name: 'Folder',
      description: 'Filter for alerts in the selected folder (only for Grafana alerts)',
      id: 'folder',
      defaultValue: null,
      editor: function RenderFolderPicker(props) {
        return (
          <OldFolderPicker
            enableReset={true}
            showRoot={false}
            allowEmpty={true}
            initialTitle={props.value?.title}
            initialFolderUid={props.value?.uid}
            permissionLevel={PermissionLevelString.View}
            onClear={() => props.onChange('')}
            {...props}
          />
        );
      },
      category: ['Filter'],
    })
    .addBooleanSwitch({
      path: 'stateFilter.firing',
      name: 'Alerting / Firing',
      defaultValue: true,
      category: ['Alert state filter'],
    })
    .addBooleanSwitch({
      path: 'stateFilter.pending',
      name: 'Pending',
      defaultValue: true,
      category: ['Alert state filter'],
    })
    .addBooleanSwitch({
      path: 'stateFilter.noData',
      name: 'No Data',
      defaultValue: false,
      category: ['Alert state filter'],
    })
    .addBooleanSwitch({
      path: 'stateFilter.normal',
      name: 'Normal',
      defaultValue: false,
      category: ['Alert state filter'],
    })
    .addBooleanSwitch({
      path: 'stateFilter.error',
      name: 'Error',
      defaultValue: true,
      category: ['Alert state filter'],
    });
});

export const plugin = config.unifiedAlertingEnabled ? unifiedAlertList : alertList;
