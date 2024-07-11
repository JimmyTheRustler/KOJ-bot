# This example requires the 'message_content' intent.

import datetime
import json
import time
import os
#from token import TOKEN
from asyncio.windows_events import NULL
from math import ceil, floor

#from functools import total_ordering
#from re import I
import discord
import gspread
import pandas as pd
import numpy as np
#from discord import Forbidden
from discord.ext import commands
from discord.utils import get
from discord import message
from gspread_dataframe import get_as_dataframe, set_with_dataframe

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='.', intents=intents)
#dev_guildId=741026554586595399
#KOJ_guildId=1129987094773776564
guildId=1129987094773776564
gc = gspread.service_account()
#main sheet
sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1gt2BVjtgzgj3DMbbPFRbDqUOajFVqZDF0YDA_CXKlQ4')
#testing sheet
#sheet = gc.open_by_url('https://docs.google.com/spreadsheets/d/1fqUcHf8TfMN1fIL2CsBPaAPK-Itfjszpwx7PF1yzPLg')
#token = TOKEN

ranks_dict = {
    'TzKal': 10000,
    'Myth': 5000,
    'Raider': 2000,
    'Completionist': 1000,
    'Gamer': 500,
    'Achiever': 250,
    'Adventurer': 125,
    'Xerician': 50,
    'Dogs Body': 0,   
}

@bot.event
async def on_ready():
    print(f'We have logged in as {bot.user}')

@bot.command()
async def test(ctx, *args):
    arguments = ', '.join(args)
    print(type(args[0]))
    await ctx.send(f'{len(args)} arguments: {arguments}')

@bot.command()
async def update_name(ctx, *args):
    arguments = ', '.join(args)
    #await com_update_name()
    await update_name_dt()

@bot.command()
async def update_months_count(ctx, *args):
    arguments = ', '.join(args)
    #await com_update_months_count()
    update_months_count_df()

@bot.command()
async def add_mems(ctx, *args):
    arguments = ', '.join(args)
    #await com_add_mems()
    await add_mems_df()

@bot.command()
async def split(ctx, user: discord.User, *args):
    #arguments = ', '.join(user)
    await add_split(ctx,user, args)
    #await add_split_df(ctx,user, args)

@bot.command()
async def attendance(ctx, user: discord.User, *args):
    #arguments = ', '.join(user)
    #await add_split_df(ctx,user, args)
    await add_event_attendance(ctx,user, *args)

@bot.command()
async def overview(ctx, *args):
    #arguments = ', '.join(user)
    #await update_overview()
    update_overview_df()


@bot.command()
async def update_ranks(ctx, *args):
    await update_ranks_df(ctx)

@bot.command()
async def theworks(ctx, *args):
    arguments = ', '.join(args)
    #await com_update_name()
    await add_mems_df()
    await update_name_dt()
    update_months_count_df()
    update_overview_df()
    print("Finished!")


async def update_ranks_df(ctx):
    overview_ws = sheet.worksheet("overview")
    pd.set_option("display.precision", 16)
    overview_ws_df = get_as_dataframe(overview_ws, dtype={'user_id': str})
    overview_ws_df = overview_ws_df.dropna()
    #print(discord.role)
    roles = ctx.guild.roles

    #get 

    for index, row in overview_ws_df.iterrows():
        row_user_id = row.loc['user_id']
        new_user_rank = row.loc['rank']
        updated = False
        old_role_id = ''
        #new_rank = overview_ws_df.loc[overview_ws_df['user_id'] == row_user_id, 'username'].iat[0]
        #seans user id
        #if row_user_id == '180884976596680704':
        try:
            user = ctx.message.guild.get_member(int(row_user_id))
            for r in user.roles:
                #print(f'{user.name}: {r} - {new_user_rank}')
                #old_role_id = r.id
                if new_user_rank in ranks_dict and str(r) != new_user_rank:
                    old_role = get(user.guild.roles, name=str(r))
                    role = get(user.guild.roles, name=new_user_rank)
                    if r != new_user_rank:
                        await user.add_roles(role)
                        await user.remove_roles(old_role)
                        await ctx.send(f'{user.display_name} just ranked up to {role}, Congrats!')

                
                
        except:
            print(f'{row}')
    print('Finished updating discord ranks')

        #except:
            #print('Found a person whos not in discord anymore')


def update_overview_df():
    dt = datetime.datetime.now()
    curr_date = dt.strftime("%Y-%m-%d %H:%M:%S")

    overview_ws = sheet.worksheet("overview")
    #print(overview_ws.col_values(1))
    member_list_ws = sheet.worksheet("member_list")
    months_count_ws = sheet.worksheet("months_count")
    splits_ws = sheet.worksheet("splits")
    attendance_ws = sheet.worksheet("event_attendance")
    pd.set_option("display.precision", 16)
    overview_ws_df = get_as_dataframe(overview_ws, dtype={'user_id': str})
    #removes empty rows
    overview_ws_df = overview_ws_df.dropna()
    months_count_ws_df = get_as_dataframe(months_count_ws, dtype={'user_id': str})
    #removes empty rows
    months_count_ws_df = months_count_ws_df.dropna()
    #print(months_count_ws_df)
    splits_ws_df = get_as_dataframe(splits_ws, dtype={'user_id': str})
    #removes empty rows
    splits_ws_df = splits_ws_df.dropna()
    attendances_ws_df = get_as_dataframe(attendance_ws, dtype={'user_id': str})
    #removes empty rows
    attendances_ws_df = attendances_ws_df.dropna()
    member_list_ws_df = get_as_dataframe(member_list_ws, dtype={'user_id': str})
    #removes empty rows
    member_list_ws_df = member_list_ws_df.dropna()

    for index, row in member_list_ws_df.iterrows():
        row_user_id = row.loc['user_id']
        row_username = row.loc['username']
        months_points = months_count_ws_df.loc[months_count_ws_df['user_id'] == row_user_id, 'points_gained'].iat[0]
        #split_points = splits_ws_df.loc[splits_ws_df['user_id'] == row_user_id, 'points_awarded'].iat[0]
        total_points = months_points + add_awarded_pts(splits_ws_df, row_user_id)
        total_points = total_points + add_awarded_pts(attendances_ws_df, row_user_id)
        months_in_clan = months_count_ws_df.loc[months_count_ws_df['user_id'] == row_user_id, 'total_months'].iat[0]
        
        rank = calc_rank(total_points, months_points)
        curr_datetime = datetime.datetime.strptime(curr_date, "%Y-%m-%d %H:%M:%S")

        #print(type(split_points))
        
        if not overview_ws_df.isin([row_user_id]).any().any():
            new_row = {'user_id': str(row_user_id), 'username': row_username, 'total_points': total_points, 'rank':  rank, 'months_in_clan': months_in_clan, 'last_modified_date': curr_date}
            overview_ws_df = pd.concat([overview_ws_df, pd.DataFrame([new_row])], ignore_index=True)

        elif overview_ws_df.isin([row_user_id]).any().any():
            #print(months_in_clan)
            overview_ws_df.loc[overview_ws_df['user_id'] == str(row_user_id), 'total_points'] = total_points
            overview_ws_df.loc[overview_ws_df['user_id'] == str(row_user_id), 'last_modified_date'] = curr_date
            overview_ws_df.loc[overview_ws_df['user_id'] == str(row_user_id), 'months_in_clan'] = str(months_in_clan)
            overview_ws_df.loc[overview_ws_df['user_id'] == str(row_user_id), 'rank'] = rank

    set_with_dataframe(overview_ws, overview_ws_df)

def add_awarded_pts(df, user_id):
    total_pts = 0
    for index, row in df.iterrows():
        if row.loc['user_id'] == user_id:
            total_pts = total_pts + row.loc['points_awarded']
    return total_pts

def calc_rank(total_pts, months):
    rank = 'Dogsbody'
    if months <= 1:
        return rank
    else:
        for key, runk in ranks_dict.items():
            #print(f'{key} - {runk}')
            if total_pts > runk:
                return key



async def add_split(ctx, user, args):
    dt = datetime.datetime.now()
    curr_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    split_amount=args[0]
    tens = dict(k=1e3, m=1e6, b=1e9)
    worksheet = sheet.worksheet("splits")
    x=2
    factor, exp = split_amount[0:-1], split_amount[-1].lower()
    split_amount = int(float(factor) * tens[exp])
    pts_award = split_amount / 1000000

    while worksheet.cell(x,1).value != None:
            x=x+1

    worksheet.update_cell(x,1,str(user.id))
    worksheet.update_cell(x,2,split_amount)
    worksheet.update_cell(x,3,floor(pts_award))
    worksheet.update_cell(x,4,curr_date)

    await ctx.send(f'Awarded {user.display_name} with {pts_award} points')

async def add_event_attendance(ctx, user, *args):
    dt = datetime.datetime.now()
    curr_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    points_awarded=args[0]
    event_type = args[1]
    worksheet = sheet.worksheet("event_attendance")
    pd.set_option("display.precision", 16)
    worksheet_df = get_as_dataframe(worksheet, dtype={'user_id': str})
    worksheet_df = worksheet_df.dropna()
    #x=2

    #while worksheet.cell(x,1).value != None:
            #x=x+1

    # worksheet.update_cell(x,1,str(user.id))
    # worksheet.update_cell(x,2,points_awarded)
    # worksheet.update_cell(x,3,event_type)
    # worksheet.update_cell(x,4,curr_date)
    #row_user_id = row.loc['user_id']
    # worksheet.loc[worksheet['user_id'] == str(row_user_id), 'points_awarded'] = points_awarded
    # worksheet.loc[worksheet['user_id'] == str(row_user_id), 'event_type'] = event_type
    # worksheet.loc[worksheet['user_id'] == str(row_user_id), 'curr_date'] = curr_date

    new_row = {'user_id': str(user.id), 'points_awarded': points_awarded, 'event_type': event_type, 'created_date':  curr_date}
    worksheet_df = pd.concat([worksheet_df, pd.DataFrame([new_row])], ignore_index=True)
    set_with_dataframe(worksheet, worksheet_df)
    await ctx.send(f'Awarded {user.display_name} with {points_awarded} points for {event_type} event!')


def update_months_count_df():
    dt = datetime.datetime.now()
    curr_date = dt.strftime("%Y-%m-%d %H:%M:%S")
    pts_per_month=10
    member_list_ws = sheet.worksheet("member_list")
    months_count_ws = sheet.worksheet("months_count")

    #disables scientific notation in dataframes
    pd.set_option("display.precision", 16)
    member_list_ws_df = get_as_dataframe(member_list_ws, dtype={'user_id': str})
    #removes empty rows
    member_list_ws_df = member_list_ws_df.dropna()

    months_count_ws_df = get_as_dataframe(months_count_ws, dtype={'user_id': str})
    #removes empty rows
    months_count_ws_df = months_count_ws_df.dropna()

    for index, row in member_list_ws_df.iterrows():
        row_user_id = row.loc['user_id']
        row_join_date = row.loc['joined_date']
        joined_datetime = datetime.datetime.strptime(row_join_date, "%Y-%m-%d %H:%M:%S")
        curr_datetime = datetime.datetime.strptime(curr_date, "%Y-%m-%d %H:%M:%S")
        total_months = (floor((curr_datetime - joined_datetime).days / 30))
        points_gained = total_months * pts_per_month


        if not months_count_ws_df.isin([row_user_id]).any().any():
            new_row = {'user_id': str(row_user_id), 'total_months': total_months, 'joined_date': row_join_date, 'modified_date':  curr_date, 'points_gained': points_gained}
            months_count_ws_df = pd.concat([months_count_ws_df, pd.DataFrame([new_row])], ignore_index=True)

        elif months_count_ws_df.isin([row_user_id]).any().any():

            months_count_ws_df.loc[months_count_ws_df['user_id'] == str(row_user_id), 'total_months'] = total_months
            months_count_ws_df.loc[months_count_ws_df['user_id'] == str(row_user_id), 'modified_date'] = curr_date
            months_count_ws_df.loc[months_count_ws_df['user_id'] == str(row_user_id), 'points_gained'] = points_gained

    set_with_dataframe(months_count_ws, months_count_ws_df)



async def update_name_dt():
    mems = []
    dt = datetime.datetime.now()
    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    member_list_ws = sheet.worksheet("member_list")
    name_changes_ws = sheet.worksheet("name_changes")

    #disables scientific notation in dataframes
    pd.set_option("display.precision", 16)
    member_list_ws_df = get_as_dataframe(member_list_ws, dtype={'user_id': str})
    #removes empty rows
    member_list_ws_df = member_list_ws_df.dropna()
    
    name_changes_ws_df = get_as_dataframe(name_changes_ws, dtype={'user_id': str})
    name_changes_ws_df = name_changes_ws_df.dropna()

    guild = await bot.fetch_guild(guildId)
    #double loop get all discord members names
    for guild in bot.guilds:
        for member in guild.members:
            try:
                old_name = member_list_ws_df.loc[member_list_ws_df['user_id'] == str(member.id), 'username']
                if old_name.iat[0] != member.display_name:
                    new_row = {'user_id': str(member.id), 'old_name': old_name.iat[0], 'new_name':  member.display_name, 'modified_date': dt_str}
                    name_changes_ws_df = pd.concat([name_changes_ws_df, pd.DataFrame([new_row])], ignore_index=True)
                    old_name = member.display_name
                    member_list_ws_df.loc[member_list_ws_df['user_id'] == str(member.id), 'username'] = member.display_name
                    member_list_ws_df.loc[member_list_ws_df['user_id'] == str(member.id), 'modified_date'] = dt_str
            except:
                print(f'Found user not in discord anymore: {member.display_name}')
    
    set_with_dataframe(member_list_ws, member_list_ws_df)
    set_with_dataframe(name_changes_ws, name_changes_ws_df)


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
async def add_mems_df():
    member_list_ws = sheet.worksheet("member_list")
    dt = datetime.datetime.now()
    dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    mems=[]
    guild = await bot.fetch_guild(guildId)
    pd.set_option("display.precision", 16)

    member_list_ws_df = get_as_dataframe(member_list_ws, dtype={'user_id': str})
    #cleans out any possible NaN rows
    member_list_ws_df = member_list_ws_df.dropna()
    #print(member_list_ws_df)


    #double loop get all discord members names
    for guild in bot.guilds:
        for member in guild.members:
            role_list = member.roles[1:]
            #print(type(role_list[0]))
            #print(role_list[0].name)
            joined_date = json.dumps(member.joined_at.strftime("%Y-%m-%d %H:%M:%S"))
            joined_date = joined_date[1:-1]
            #mems.append([member.id, member.display_name, member.joined_at, role_list])
            #print(f'{member.display_name} has the roles: {member.roles[1:]}')
            if not member_list_ws_df.isin([str(member.id)]).any().any():
                new_row = {'user_id': str(member.id), 'username': member.display_name, 'modified_date':  dt_str, 'joined_date': joined_date}
                member_list_ws_df = pd.concat([member_list_ws_df, pd.DataFrame([new_row])], ignore_index=True)
    set_with_dataframe(member_list_ws, member_list_ws_df)
            

bot.run('')
