%%queue thresh calculation

function thresh = QueueThreshCalulation()
   global randnum;
   global link_rate;
   global pkt_size;
   global lambda;
   
   link_rate = 10;
   link_load = 0.6;
   pkt_size = 1460;
   
   population = 100;
   queue_num = 8;
   add_ratio = 1.1;
   exchange_num = 2;
   mute_range = 10;
   mute_ratio = [1 1 2 5 10 100 1000];
   stop_ratio = 1e-12;
   stop_genenum = 100;
   new_ratio = 0.95; 
   
    %queue_size = 240;
    %meanFlowSize = 5117*1460;
    %meanFlowSize = 5117;
    %lambda = (link_rate*load*1000000000)/(meanFlowSize*8.0/1460*1500);%why is 8.0
    % paretoShape = 1.05;
    % paretoSigma = meanFlowSize * (paretoShape - 1) / paretoShape;
    % paretoTheta = paretoSigma / paretoShape;
    % 
    % %%generate pareto number 
    % randnum_len = 1000000;
    % randnum = [];
    % while(randnum_len > 1)
    %     randnum_temp = gprnd(paretoShape, paretoSigma, 0.5);
    %     randnum = [randnum randnum_temp];
    %     randnum_len = randnum_len - 1;
    % end

    load flow_size_vl2.txt;
    randnum = flow_size_vl2;
    meanFlowSize = mean(randnum);
    lambda = (link_rate*link_load*1000000000)/(meanFlowSize*8.0/1460*1500);
    
    [thresh fitness] = GeneAlgPIAS(population, flow_size_vl2, queue_num, add_ratio, exchange_num, mute_range, mute_ratio, stop_genenum, stop_ratio, new_ratio);
    
end


function [thresh fitness] = GeneAlgPIAS(population, flow_size, queue_num, add_ratio, exchange_num, mute_range, mute_ratio, stop_genenum, stop_ratio, new_ratio)
    global group_fitness;

    group = GeneInit(population, flow_size, queue_num);
    group_fitness = GetGroupFitness(group);
    new_group = GeneNewGroup(group, exchange_num, mute_range, mute_ratio, add_ratio, stop_ratio, new_ratio);
    while var(group_fitness) >= stop_ratio && stop_genenum > 0
        stop_genenum = stop_genenum - 1;
        group = new_group;
        group_fitness_select = (max(group_fitness) - group_fitness) * 1e+10;
        group_nomax = [];
        group_fitness_nomax = [];
        for i = 1:size(group, 1)
            if group_fitness_select(i) ~= 0
                group_nomax = [group_nomax; group(i,:)];
                group_fitness_nomax = [group_fitness_nomax group_fitness(i)];
            end
        end
        
        if (length(group_fitness) - length(group_fitness_nomax)) > 1
            group_fitness = group_fitness_nomax;
            group = group_nomax;
        end
        
        if var(group_fitness) < stop_ratio
            new_group = group;
        else
            new_group = GeneNewGroup(group, exchange_num, mute_range, mute_ratio, add_ratio, stop_ratio, new_ratio);
        end
        
        fprintf('group length : %d\n', size(group, 1));
        fprintf('new group length : %d\n', size(new_group, 1));
    end
    
    %group_fitness = GetGroupFitness(group);
    group = new_group;
    fitness = min(group_fitness);
    index = 0;
    for i = 1:length(group_fitness)
        if fitness == group_fitness(i)
            index = i;
            break;
        end
    end
    thresh = group(index, :);
    stop_genenum
end

function group = GeneInit(population, flow_size, queue_num)
    group = [];
    for i = 1:population
        fprintf('gene init begin %d\n', i);
%         group_one = randi(max(flow_size), 1, (queue_num - 1));
%         group_one = [group_one 0];
%         group_one = sort(group_one);
        group_one = [1:1:8];
        group_one(1) = 0;
        group_one(2) = randi(500, 1, 1);
        group_one(3) = randi(2000, 1, 1) + 500;
        group_one(4) = randi(2000, 1, 1) + 500;
        group_one(5) = randi(10000, 1, 1) + 2500;
        group_one(6) = randi(10000, 1, 1) + 5000;
        group_one(7) = randi(10000, 1, 1) + 10000;
        group_one(8) = randi(20000, 1, 1) + 20000;
        group_one = sort(group_one);
        group = [group; group_one];
    end
end

function select_member_index = GeneSelect(group)
    global group_fitness;
    
    fprintf('group select begin %d\n', size(group, 1));
    %fitness = GetGroupFitness(group);
    group_fitness_select = (max(group_fitness) - group_fitness) * 1e+10;
    rand_range = round(sum(group_fitness_select));
    
    if rand_range == 0
        randnum_fit = randi(rand_range, 1, 1);
    else
        randnum_fit = 0;
    end
    for j = 1:length(group_fitness_select)
        randnum_fit = randnum_fit - group_fitness_select(j);
        if randnum_fit < 0
             select_member_index = j;
             break;
        end
    end
    fprintf('group select end %d\n', size(group, 1));
end

function new_member = GeneMute(group, mother, dad, exchange_num, mute_range, mute_ratio)
    global group_fitness;

    fprintf('gene mute begin %d\n', mother);
    index = [];

    for i = 1:exchange_num
        index_temp = mod(round(rand*10000), size(group, 1)) + 1;
        index = [index index_temp];
    end
    
    if group_fitness(mother) > group_fitness(dad) 
        new_member = group(mother, :);
    else
        new_member = group(dad, :);
    end
    
    if all(new_member==dad)
        dad = group(mother, :);
        mother = new_member;
    else
        mother = group(mother, :);
        dad = group(dad, :);
    end
    
    flag = 0;
    for i = 1:length(index)
        index_temp = index(i);
        if index_temp == length(mother)
            if dad(index_temp) > mother(index_temp - 1)
                new_member(index_temp) = dad(index_temp);
                flag = 1;
            end
        elseif index_temp > 1 && index_temp < length(mother)
            if dad(index_temp) > mother(index_temp - 1) && dad(index_temp) < mother(index_temp + 1)
                new_member(index_temp) = dad(index_temp);
                flag = 1;
            end
        end
    end
    
    if flag == 0 || all(mother==dad)
        mute_num = 0;
        for i = 2:length(mother)
            mute_temp = mod((round(rand*10000)), mute_range) - mute_range/2;
            mute_temp = mute_temp * mute_ratio(i - 1);
            if i < length(mother) && mute_temp + new_member(i) > new_member(i - 1) && mute_temp + new_member(i) < new_member(i + 1)
                mute_num = [mute_num mute_temp];
            elseif i == length(mother) && mute_temp + new_member(i) > new_member(i - 1)
                mute_num = [mute_num mute_temp];
            else
                mute_num = [mute_num 0];
            end
        end
        new_member = new_member + mute_num;
    end
    fprintf('gene mute end %d\n', flag);
end

function new_group = GeneNewGroup(group, exchange_num, mute_range, mute_ratio, add_ratio, stop_ratio, new_ratio)
    global group_fitness;

    fprintf('get new group begin %d\n', size(group, 1));
    fprintf('get group_fitness begin %d\n', length(group_fitness));
    %group_fitness = GetGroupFitness(group);
    %group_fitness = (max(group_fitness) - group_fitness) * 1e+10;
   
    new_group_length = round(add_ratio * size(group, 1));
    new_group_temp = group;
    new_group_fitness = group_fitness;
    for i = 1:new_group_length
       select_mother = GeneSelect(group);
       select_dad = GeneSelect(group);
 
       new_member = GeneMute(group, select_mother, select_dad, exchange_num, mute_range, mute_ratio);
       new_group_temp = [new_group_temp; new_member];
       new_group_fitness = [new_group_fitness FitnessCal(new_member)];
       
       new_group_temp
       fitness = min(new_group_fitness)
       index = 0;
       for j = 1:length(new_group_fitness)
          if fitness == new_group_fitness(j)
            index = j;
            break;
          end
       end
       thresh = new_group_temp(index, :)
       
    end
    group_fitness = new_group_fitness;
    fprintf('get group_fitness mid1 %d\n', length(group_fitness));
    
    new_group = [];
    new_group_fitness_temp = [];
    %group_fitness = GetGroupFitness(group);
    
    if var(new_group_fitness) < stop_ratio
        new_group = new_group_temp;
    else
        group_fitness_temp = new_group_fitness - mean(new_group_fitness);
        group_fitness_sort = sort(group_fitness_temp, 'descend');
        for i = 1:round(new_ratio * size(new_group_temp, 1))
            find_temp = group_fitness_sort(i);
            for j = 1 : size(new_group_temp, 1)
                if find_temp == group_fitness_temp(j)
                    find_index = j;
                    continue;
                end
            end
            new_group = [new_group; new_group_temp(find_index, :)];
            new_group_fitness_temp = [new_group_fitness_temp new_group_fitness(find_index)];
        end
        group_fitness = new_group_fitness_temp;
    end
    fprintf('get group_fitness mid2 %d\n', length(group_fitness));
    %group_fitness = new_group_fitness_temp;
    fprintf('get new group end %d\n', size(new_group, 1));
    fprintf('get group_fitness end %d\n', length(group_fitness));
end

function group_fitness_get = GetGroupFitness(group)
    fprintf('get group fitness begin %d\n', size(group, 1));
    group_fitness_get = [];
    for i = 1:size(group, 1)
        fprintf('solve fitness begin %d\n', i);
        group_fitness_temp = FitnessCal(group(i, :));
        group_fitness_get = [group_fitness_get group_fitness_temp];
    end
    fprintf('get group fitness end %d\n', size(group, 1));
end

function fitness = FitnessCal(member)
    global randnum;
    global link_rate;
    global pkt_size;
    global lambda;

    thresh = member;
    [queue_lambda queue_theta] = QueueThetaCal(thresh, randnum);

    for i = 1:length(queue_lambda)
        if i == length(queue_lambda)
            queue_lambda(i) = lambda * (max(randnum) - thresh(i)) * queue_theta(i);
        else 
            queue_lambda(i) = lambda * (thresh(i + 1) - thresh(i)) * queue_theta(i);
        end
    end

    service_rate = link_rate * (1000000000 / pkt_size);
    queue_omega = [service_rate];
    for i = 2:length(queue_lambda)
        queue_omega_temp = [];
        for j = 2:i
            queue_omega_temp = [queue_omega_temp (1 - queue_lambda(j - 1) / queue_omega(j - 1))];
        end
        queue_omega = [queue_omega prod(queue_omega_temp) * service_rate];
    end

    queue_delay = [];
    for i = 1:length(queue_lambda)
        queue_delay_temp = 1 / (queue_omega(i) - queue_lambda(i));
        queue_delay = [queue_delay queue_delay_temp];
    end

    temp = [];
    for i = 1:length(queue_lambda)
        temp_delay = [];
        for j = 1:i
            temp_delay = [temp_delay queue_delay(j)];
            temp = [temp queue_theta(j) * sum(temp_delay)];
        end
        temp = [temp queue_theta(j) * sum(temp_delay)];
    end
    fitness = sum(temp);
end


function [queue_lambda queue_theta] = QueueThetaCal(thresh, x)
    queue_theta = [];
    queue_lambda = [];
    for i = 1:length(thresh)
        if i == length(thresh)            
            [size percent] = CdfPias(thresh(i), -1, x);
            queue_theta = [queue_theta percent];
            queue_lambda = [queue_lambda size];
        else
            [size percent] = CdfPias(thresh(i), thresh(i + 1), x);
            queue_theta = [queue_theta percent];
            queue_lambda = [queue_lambda size];
        end
    end
    
end

function [queue_meanflowsize percent] = CdfPias(start_value, end_value, x)
    totalnum = length(x);
    find_num = [];
    for i = 1:totalnum
        if start_value == -1 && x(i) < end_value
            find_num = [find_num x(i)]; 
        elseif x(i) > start_value && end_value == -1
            find_num = [find_num x(i)];
        elseif x(i) > start_value && x(i) < end_value
            find_num = [find_num x(i)];
        end    
    end
    
    percent = length(find_num) / totalnum;
    queue_meanflowsize = sum(find_num) / totalnum;
end





