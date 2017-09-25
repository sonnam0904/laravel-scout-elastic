<?php

namespace ScoutEngines\Elasticsearch;

use Laravel\Scout\Builder;
use Laravel\Scout\Engines\Engine;
use Elasticsearch\Client as Elastic;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Collection as BaseCollection;

class ElasticsearchEngine extends Engine
{
    /**
     * Index where the models will be saved.
     *
     * @var string
     */
    protected $index;

    /**
     * Create a new engine instance.
     *
     * @param  \Elasticsearch\Client  $elastic
     * @return void
     */
    public function __construct(Elastic $elastic, $index)
    {
        $this->elastic = $elastic;
        $this->index = $index;
    }

    /**
     * Update the given model in the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function update($models)
    {
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'update' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                ]
            ];
            $params['body'][] = [
                'doc' => $model->toSearchableArray(),
                'doc_as_upsert' => true
            ];
        });

        $this->elastic->bulk($params);
    }

    /**
     * Remove the given model from the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'delete' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    '_type' => $model->searchableAs(),
                ]
            ];
        });

        $this->elastic->bulk($params);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, array_filter([
            'numericFilters' => $this->filters($builder),
            'ranger' => $this->filtersRanger($builder),
            'size' => $builder->limit
        ]));
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        $result = $this->performSearch($builder, [
            'numericFilters' => $this->filters($builder),
            'ranger' => $this->filtersRanger($builder),
            'from' => (($page * $perPage) - $perPage),
            'size' => $perPage,
        ]);

       $result['nbPages'] = $result['hits']['total']/$perPage;

        return $result;
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  array  $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        $extraSearch = '';
        if (isset($options['numericFilters']) && count($options['numericFilters'])) {
            $extraSearch = $options['numericFilters'];
        }
        $params = [
            'index' => $this->index,
            'type' => $builder->model->searchableAs(),
            'body' => [
				'track_scores' => true,
                'query' => [
                    'bool' => [
                        'must' => [
                            'query_string' => [
                                'query' => "+*{$builder->query}*$extraSearch"
                            ]
                        ]
                    ]
                ]
            ]
        ];

        // 'query' => "*{$builder->query}* +(category_id:(17 OR 56)) +(location_id:(1))"

        if ($this->priority($builder->model->searchableAs()))
        {
            $params['body']['query']['bool']['must']['query_string']['fields'] = $this->priority($builder->model->searchableAs());
            $params['body']['query']['bool']['must']['query_string']['use_dis_max'] = true;
        }

        if ($this->filterDate($builder->model->searchableAs(), ['ranger' => $options['ranger']]))
        {
            $params['body']['query']['bool']['filter']['range'] = $this->filterDate($builder->model->searchableAs(), ['ranger' => $options['ranger']]);
        }

        if ($sort = $this->sort($builder)) {
            $params['body']['sort'] = $sort;
        }
        if (isset($options['from'])) {
            $params['body']['from'] = $options['from'];
        }
        if (isset($options['size'])) {
            $params['body']['size'] = $options['size'];
        }
//        dd($params);
//
        return $this->elastic->search($params);
    }

    /**
     * @param $type
     * @return array
     */
    protected function filterDate($type, $extra = [])
    {
        switch ($type)
        {
            case 'posts_index' :
                return [
                    'up_time' => [
                        'gt' => time() - 86400*30*6
                    ]
                ];
            case 'storage_item_index' :
            {
                if (isset($extra['ranger']) && $extra['ranger'])
                {
                    return $extra['ranger'];
                }
                return false;
            }
            default:
                return false;
        }
        return false;
    }

    /**
     * @param $type
     * @return array
     */
    protected function priority($type)
    {
        switch ($type)
        {
            case 'posts_index' :
                return [ "title^100", "message"];
            case 'storage_item_index' :
                return [ "item_name^100", "item_desc"];
            default:
                return false;
        }
        return false;
    }

    /**
     * Generates the sort if theres any.
     *
     * @param  Builder $builder
     * @return array|null
     */
    protected function sort($builder)
    {
        if (count($builder->orders) == 0) {
            return null;
        }
        return collect($builder->orders)->map(function($order) {
            return [$order['column'] => $order['direction']];
        })->toArray();
    }


    /**
     * Get the filter array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        $extraSearch = '';
        foreach ($builder->wheres AS $key => $value)
        {
            $build = true;
            if (is_array($value))
            {
                foreach ($value AS $k => $v)
                {
                    if (in_array($k, ['>', '<', '>=', '<=']) && $k)
                    {
                        // ignore operator
                        $build = false;
                    }
                }

                if ($build)
                {
                    $extraSearch .= " +(";
                    $condition = '';
                    foreach ($value AS $k => $v)
                    {
                        $condition .= "($key:($v)) OR ";
                    }
                    $condition = substr($condition, 0, -4);
                    $extraSearch .= $condition . ")";
                }
            }
            else
            {
                $extraSearch .= " +($key:($value))";
            }
            unset($build);
        }
        return $extraSearch;
    }

    /**
     * Get the filter ranger array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function filtersRanger(Builder $builder)
    {
        $extraSearch = [];
        foreach ($builder->wheres AS $key => $value)
        {
            if (is_array($value))
            {
                foreach ($value AS $operator => $data)
                {
                    if (($operator === '<' || $operator === '>' || $operator === '<=' || $operator === '>=') && $data>0)
                    {
                        $extraSearch[$key][$this->_convertOperator($operator)] = $data;
                        //$extraSearch['create_date']['lt'] = time() - 86400*30*6; // chi hien thi san pham update 6 thang truoc
                    }
                }
            }
        }
        return $extraSearch;
    }

    /**
     * @param $operator
     * @return bool|string
     */
    protected function _convertOperator($operator)
    {
        switch ($operator)
        {
//            case '>' :
//                return 'lte';
//            case '>=' :
//                return 'lt';
//            case '<' :
//                return 'gte';
//            case '<=' :
//                return 'gt';

            case '>' :
                return 'gt';
            case '>=' :
                return 'gte';
            case '<' :
                return 'lt';
            case '<=' :
                return 'lte';

            default:
                return false;
        }
        return false;
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
    public function mapIds($results)
    {
        return collect($results['hits']['hits'])->pluck('_id')->values();
    }

    /**
     * Map the given results to instances of the given model.
     *
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return Collection
     */
    public function map($results, $model)
    {
        if (count($results['hits']['total']) === 0) {
            return Collection::make();
        }

        $keys = collect($results['hits']['hits'])
                        ->pluck('_id')->values()->all();

        $models = $model->whereIn(
            $model->getKeyName(), $keys
        )->get()->keyBy($model->getKeyName());

        if (count($keys) > count($models))
        {
            // missing item in DB -> delete index
            foreach ($keys AS $k => $v)
            {
                if (!isset($models[$v]))
                {
                    $params['body'] = [];

                    $params['body'][] = [
                        'delete' => [
                            '_id' => $v,
                            '_index' => $this->index,
                            '_type' => $model->searchableAs(),
                        ]
                    ];

                    $this->elastic->bulk($params);
                }
            }
        }

        return collect($results['hits']['hits'])->map(function ($hit) use ($model, $models) {
            return $models[$hit['_id']];
        });
    }

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
    public function getTotalCount($results)
    {
        return $results['hits']['total'];
    }
}
