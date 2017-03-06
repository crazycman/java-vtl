/*global define*/
'use strict';

define(['angular', 'require'], function (angular, req) {
    var moduleName = 'vtl.tree';
    angular.module(moduleName, [])
        .directive('vtlTree', function () {
            return {
                restrict: 'E',
                scope: {
                    tree: '=',
                },
                replace: true,
                templateUrl: req.toUrl('./vtl-tree.html'),
                controller: ['$scope', '$http', function ($scope, $http) {
                    $scope.opts = {
                        injectClasses: {
                            //"ul": "c-ul",
                            //"li": "c-li",
                            //"liSelected": "c-liSelected",
                            "iExpanded": "icon i-16-bullet_arrow_down",
                            "iCollapsed": "icon i-16-bullet_arrow_right",
                            "iLeaf": "",
                            "label": "c-label",
                            "labelSelected": "c-labelSelected"
                        }
                    };
                }]
            };
        });
    return moduleName;
});
