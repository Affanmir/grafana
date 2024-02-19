import { PanelPlugin } from '@grafana/data';

import { NodeGraphPanel } from './NodeGraphPanel';
import { ArcOptionsEditor } from './editor/ArcOptionsEditor';
import { Options } from './panelcfg.gen';

export const plugin = new PanelPlugin<Options>(NodeGraphPanel).setPanelOptions((builder, context) => {
  builder.addNestedOptions({
    category: ['Nodes'],
    path: 'nodes',
    build: (builder) => {
      builder.addUnitPicker({
        name: 'Main stat unit',
        path: 'mainStatUnit',
      });
      builder.addUnitPicker({
        name: 'Secondary stat unit',
        path: 'secondaryStatUnit',
      });
      builder.addCustomEditor({
        name: 'Arc sections',
        path: 'arcs',
        id: 'arcs',
        editor: ArcOptionsEditor,
      });
    },
  });
  builder.addNestedOptions({
    category: ['Edges'],
    path: 'edges',
    build: (builder) => {
      builder.addUnitPicker({
        name: 'Main stat unit',
        path: 'mainStatUnit',
      });
      builder.addUnitPicker({
        name: 'Secondary stat unit',
        path: 'secondaryStatUnit',
      });
    },
  });
});
